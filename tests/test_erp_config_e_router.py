"""
Testes da config Omie + roteador ERP em api_send.

Cobre:
- _build_erp_importer retorna TinyImporter ou OmieImporter
- _load_omie_config / _save_omie_config persistem corretamente
- Endpoint GET /master/api/unidades/<slug>/omie-config retorna mascarado
- Endpoint POST salva e preserva app_secret quando omitido
- _is_doc_already_registered cobre mensagens Omie
- envios_erp grava com erp correto via api_send
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

os.environ.setdefault("SECRET_KEY", "test-secret-key-pytest")
os.environ.setdefault("USERS_CONFIG", "{}")
os.environ.setdefault("UNITS_CONFIG", "{}")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import server  # noqa: E402
from omie_import import OmieImporter, _is_omie_redundant_error, _is_omie_misuse_error  # noqa: E402
from tiny_import import TinyImporter, _is_doc_already_registered  # noqa: E402


UNITS_FIX = {
    "barueri":     {"nome": "Barueri",     "erp": "tiny"},
    "indianopolis": {"nome": "Indianópolis", "erp": "omie"},
}
MASTER_USER = {"email": "admin@astro.com", "name": "Admin", "unit": None, "master": True}


@pytest.fixture
def master_client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    # _UNITS_CUSTOM_FILE eh resolvido no import — redirecionar pra tmp_path
    server._UNITS_CUSTOM_FILE = tmp_path / "units_custom.json"
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=MASTER_USER):
        with server.app.test_client() as c:
            yield c


# ══════════════════════════════════════════════════════════════════════════════
# _is_doc_already_registered (cobre Omie tambem)
# ══════════════════════════════════════════════════════════════════════════════

class TestDocAlreadyRegistered:
    def test_tiny_msg_classica(self):
        assert _is_doc_already_registered(Exception("ja cadastrado no sistema"))

    def test_omie_ja_consta_cadastrado(self):
        assert _is_doc_already_registered(Exception("codigo_lancamento_integracao ja consta cadastrado"))

    def test_omie_codigo_lancamento_integracao(self):
        assert _is_doc_already_registered(Exception("codigo_lancamento_integracao 123 ja existe cadastrado"))

    def test_msg_alheia_retorna_false(self):
        assert not _is_doc_already_registered(Exception("erro de rede"))


# ══════════════════════════════════════════════════════════════════════════════
# _build_erp_importer (roteador)
# ══════════════════════════════════════════════════════════════════════════════

class TestRouter:
    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path):
        server.DATA_DIR = tmp_path
        server.UNITS = UNITS_FIX

    def test_unit_tiny_retorna_TinyImporter(self):
        config = server._build_unit_config("barueri")
        state_dir = server._unit_state_dir("barueri")
        with patch.object(server, "_seed_tokens"):  # nao mexer em tokens em teste
            importer, kind = server._build_erp_importer("barueri", config, state_dir)
        assert kind == "tiny"
        assert isinstance(importer, TinyImporter)

    def test_unit_omie_retorna_OmieImporter(self):
        config = server._build_unit_config("indianopolis")
        state_dir = server._unit_state_dir("indianopolis")
        importer, kind = server._build_erp_importer("indianopolis", config, state_dir)
        assert kind == "omie"
        assert isinstance(importer, OmieImporter)

    def test_unit_sem_erp_default_tiny(self, tmp_path):
        server.UNITS = {"x": {"nome": "X"}}  # sem campo erp
        config = server._build_unit_config("x")
        state_dir = server._unit_state_dir("x")
        with patch.object(server, "_seed_tokens"):
            _, kind = server._build_erp_importer("x", config, state_dir)
        assert kind == "tiny"


# ══════════════════════════════════════════════════════════════════════════════
# _load_omie_config / _save_omie_config
# ══════════════════════════════════════════════════════════════════════════════

class TestOmieConfigPersist:
    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path):
        server.DATA_DIR = tmp_path
        server.UNITS = UNITS_FIX

    def test_load_inexistente_retorna_dict_vazio(self):
        assert server._load_omie_config("indianopolis") == {}

    def test_save_e_load_roundtrip(self):
        cfg = {
            "app_key":           "KEY",
            "app_secret":        "SECRET",
            "id_conta_corrente": 12345,
            "categoria_ids":     {"VISTORIA CAUTELAR": "1.01.01"},
        }
        server._save_omie_config("indianopolis", cfg)
        loaded = server._load_omie_config("indianopolis")
        assert loaded["app_key"] == "KEY"
        assert loaded["app_secret"] == "SECRET"
        assert loaded["id_conta_corrente"] == 12345
        assert loaded["categoria_ids"] == {"VISTORIA CAUTELAR": "1.01.01"}

    def test_save_filtra_chaves_desconhecidas(self):
        server._save_omie_config("indianopolis", {
            "app_key": "K", "lixo": "x", "outra_coisa": 123
        })
        on_disk = json.loads(server._omie_config_path("indianopolis").read_text())
        assert "lixo" not in on_disk
        assert "outra_coisa" not in on_disk

    def test_id_conta_corrente_invalido_vira_zero(self):
        server._save_omie_config("indianopolis", {"id_conta_corrente": "abc"})
        assert server._load_omie_config("indianopolis")["id_conta_corrente"] == 0

    def test_categoria_ids_normaliza_uppercase(self):
        server._save_omie_config("indianopolis", {
            "categoria_ids": {"vistoria cautelar": "1.01.01"}
        })
        loaded = server._load_omie_config("indianopolis")
        assert "VISTORIA CAUTELAR" in loaded["categoria_ids"]


# ══════════════════════════════════════════════════════════════════════════════
# Endpoints
# ══════════════════════════════════════════════════════════════════════════════

class TestEndpointOmieConfig:
    def test_get_unidade_invalida_retorna_400(self, master_client):
        r = master_client.get("/master/api/unidades/inexistente/omie-config")
        assert r.status_code == 400

    def test_get_retorna_config_mascarada(self, master_client):
        server._save_omie_config("indianopolis", {
            "app_key": "MEUKEY", "app_secret": "SUPER_SECRET_LONGO",
            "id_conta_corrente": 999,
        })
        r = master_client.get("/master/api/unidades/indianopolis/omie-config")
        assert r.status_code == 200
        cfg = r.get_json()["config"]
        assert cfg["app_key"] == "MEUKEY"
        assert cfg["has_app_secret"] is True
        assert "***" in cfg["app_secret_masked"]
        assert cfg["id_conta_corrente"] == 999
        # app_secret cru NAO vai no payload
        assert "app_secret" not in cfg

    def test_post_salva_credenciais(self, master_client):
        r = master_client.post("/master/api/unidades/indianopolis/omie-config", json={
            "app_key": "NOVO_KEY", "app_secret": "NOVO_SECRET",
            "id_conta_corrente": 42,
        })
        assert r.status_code == 200
        loaded = server._load_omie_config("indianopolis")
        assert loaded["app_key"] == "NOVO_KEY"
        assert loaded["app_secret"] == "NOVO_SECRET"
        assert loaded["id_conta_corrente"] == 42

    def test_post_sem_app_secret_preserva_atual(self, master_client):
        # Salva inicial
        server._save_omie_config("indianopolis", {
            "app_key": "K1", "app_secret": "S1", "id_conta_corrente": 1
        })
        # POST sem app_secret — deve preservar S1
        master_client.post("/master/api/unidades/indianopolis/omie-config", json={
            "app_key": "K2", "id_conta_corrente": 2
        })
        loaded = server._load_omie_config("indianopolis")
        assert loaded["app_key"] == "K2"
        assert loaded["app_secret"] == "S1"  # preservado
        assert loaded["id_conta_corrente"] == 2

    def test_pagina_html_serve(self, master_client):
        r = master_client.get("/master/erp-config")
        assert r.status_code == 200
        assert b"Configura" in r.data


# ══════════════════════════════════════════════════════════════════════════════
# Endpoint trocar ERP
# ══════════════════════════════════════════════════════════════════════════════

class TestTrocarErp:
    def test_troca_tiny_pra_omie_persiste(self, master_client):
        r = master_client.post("/master/api/unidades/barueri/erp", json={"erp": "omie"})
        assert r.status_code == 200
        body = r.get_json()
        assert body["success"] is True
        assert body["erp"] == "omie"
        # Persiste em units_custom.json + recarrega UNITS
        custom = server._load_units_custom()
        assert custom["barueri"]["erp"] == "omie"

    def test_erp_invalido_retorna_400(self, master_client):
        r = master_client.post("/master/api/unidades/barueri/erp", json={"erp": "bling"})
        assert r.status_code == 400

    def test_unit_inexistente_retorna_400(self, master_client):
        r = master_client.post("/master/api/unidades/inexistente/erp", json={"erp": "omie"})
        assert r.status_code == 400

    def test_mesmo_erp_eh_noop(self, master_client):
        r = master_client.post("/master/api/unidades/barueri/erp", json={"erp": "tiny"})
        assert r.status_code == 200
        assert r.get_json().get("noop") is True


# ══════════════════════════════════════════════════════════════════════════════
# Endpoint /master/api/erp-stats
# ══════════════════════════════════════════════════════════════════════════════

class TestErpStats:
    def test_sem_envios_retorna_zeros(self, master_client):
        r = master_client.get("/master/api/erp-stats?dias=7")
        assert r.status_code == 200
        body = r.get_json()
        assert body["totais"]["tiny"]["count"] == 0
        assert body["totais"]["omie"]["count"] == 0
        # Tem linha pra cada unit
        assert len(body["por_unidade"]) == len(UNITS_FIX)

    def test_periodo_obedece_param_dias(self, master_client):
        r = master_client.get("/master/api/erp-stats?dias=14")
        body = r.get_json()
        assert body["periodo"]["dias"] == 14

    def test_dias_invalido_clampa(self, master_client):
        r = master_client.get("/master/api/erp-stats?dias=9999")
        assert r.status_code == 200
        assert r.get_json()["periodo"]["dias"] <= 365

    def test_pagina_comparativo_serve(self, master_client):
        r = master_client.get("/master/erp-comparativo")
        assert r.status_code == 200
        assert b"Comparativo" in r.data


# ══════════════════════════════════════════════════════════════════════════════
# _is_omie_misuse_error / _is_omie_redundant_error
# ══════════════════════════════════════════════════════════════════════════════

class TestOmieErrorDetection:
    def test_misuse_detecta_faultstring(self):
        assert _is_omie_misuse_error(Exception("MISUSE_API_PROCESS: API bloqueada por consumo indevido"))

    def test_misuse_detecta_consumo_indevido(self):
        assert _is_omie_misuse_error(Exception("consumo indevido detectado"))

    def test_misuse_detecta_bloqueada(self):
        assert _is_omie_misuse_error(Exception("API bloqueada"))

    def test_misuse_nao_dispara_em_redundant(self):
        assert not _is_omie_misuse_error(Exception("Consumo redundante detectado"))

    def test_misuse_nao_dispara_em_erro_generico(self):
        assert not _is_omie_misuse_error(Exception("erro de rede"))

    def test_redundant_detecta_msg_padrao(self):
        assert _is_omie_redundant_error(Exception("Consumo redundante detectado. Aguarde 57 segundos (REDUNDANT)"))

    def test_redundant_nao_dispara_em_misuse(self):
        assert not _is_omie_redundant_error(Exception("MISUSE_API_PROCESS"))


# ══════════════════════════════════════════════════════════════════════════════
# Cache persistente de clientes
# ══════════════════════════════════════════════════════════════════════════════

class TestOmieContactCache:
    @pytest.fixture
    def importer(self, tmp_path):
        server.DATA_DIR = tmp_path
        server.UNITS = UNITS_FIX
        config = {"omie": {"app_key": "K", "app_secret": "S", "id_conta_corrente": 1,
                           "categoria_ids": {"VISTORIA": "1.01.01"}}}
        return OmieImporter(config, tmp_path)

    def test_cache_vazio_no_inicio(self, importer):
        assert importer._contact_cache == {}

    def test_save_e_load_roundtrip(self, importer, tmp_path):
        from tiny_import import normalize_key
        key = normalize_key("JOAO|12345678901")
        importer._contact_cache[key] = 999
        importer._save_contact_cache()
        cache_path = tmp_path / "omie_contact_cache.json"
        assert cache_path.exists()
        imp2 = OmieImporter(importer.config, tmp_path)
        assert imp2._contact_cache[key] == 999

    def test_resolve_contact_usa_cache(self, importer):
        from tiny_import import normalize_key
        key = normalize_key("CARLOS ALBERTO DE PINHO|")
        importer._contact_cache[key] = 777
        cid = importer.resolve_contact("CARLOS ALBERTO DE PINHO")
        assert cid == 777

    def test_cache_sobrevive_entre_instancias(self, importer, tmp_path):
        from tiny_import import normalize_key
        key = normalize_key("HLM MOTORS|")
        importer._contact_cache[key] = 555
        importer._save_contact_cache()
        imp2 = OmieImporter(importer.config, tmp_path)
        assert imp2._contact_cache.get(key) == 555

    def test_cache_corrompido_retorna_vazio(self, tmp_path):
        cache_path = tmp_path / "omie_contact_cache.json"
        cache_path.write_text("NOT JSON", encoding="utf-8")
        config = {"omie": {"app_key": "K", "app_secret": "S"}}
        imp = OmieImporter(config, tmp_path)
        assert imp._contact_cache == {}


# ══════════════════════════════════════════════════════════════════════════════
# Bloqueio server-side MISUSE (429)
# ══════════════════════════════════════════════════════════════════════════════

class TestOmieMisuseBlock:
    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path):
        server.DATA_DIR = tmp_path
        server.UNITS = UNITS_FIX
        server._UNITS_CUSTOM_FILE = tmp_path / "units_custom.json"
        server._OMIE_BLOCKED_UNTIL.clear()
        yield
        server._OMIE_BLOCKED_UNTIL.clear()

    def test_bloqueio_retorna_429(self):
        import time as _time
        server._OMIE_BLOCKED_UNTIL["indianopolis"] = _time.time() + 1800
        server.app.config["TESTING"] = True
        with patch.object(server, "_current_user", return_value=MASTER_USER):
            with server.app.test_client() as c:
                r = c.post("/u/indianopolis/api/send",
                           json={"records": []},
                           headers={"X-CSRF-Token": "test"})
                assert r.status_code == 429
                body = r.get_json()
                assert body["success"] is False
                assert "bloqueado" in body["error"].lower()

    def test_sem_bloqueio_nao_retorna_429(self):
        server.app.config["TESTING"] = True
        with patch.object(server, "_current_user", return_value={"email": "op@astro.com", "unit": "indianopolis"}):
            with patch.object(server, "_build_erp_importer", return_value=(MagicMock(), "omie")):
                with server.app.test_client() as c:
                    r = c.post("/u/indianopolis/api/send",
                               json={"records": []},
                               headers={"X-CSRF-Token": "test"})
                    assert r.status_code == 200


# ══════════════════════════════════════════════════════════════════════════════
# Prefetch clientes
# ══════════════════════════════════════════════════════════════════════════════

class TestOmiePrefetch:
    def test_prefetch_popula_cache(self, tmp_path):
        config = {"omie": {"app_key": "K", "app_secret": "S"}}
        imp = OmieImporter(config, tmp_path)
        fake_response = {
            "clientes_cadastro": [
                {"razao_social": "HLM MOTORS", "cnpj_cpf": "12345678901", "codigo_cliente_omie": 111},
                {"razao_social": "RDVS AUTO", "cnpj_cpf": "", "codigo_cliente_omie": 222},
            ],
            "total_de_paginas": 1,
        }
        with patch.object(imp.client, "request", return_value=fake_response):
            total = imp.prefetch_all_contacts()
        from tiny_import import normalize_key
        assert total == 2
        assert imp._contact_cache.get(normalize_key("HLM MOTORS|12345678901")) == 111
        assert imp._contact_cache.get(normalize_key("RDVS AUTO|")) == 222
        assert (tmp_path / "omie_contact_cache.json").exists()


# ══════════════════════════════════════════════════════════════════════════════
# Cache de contratos (nCodCli → nCodCtr)
# ══════════════════════════════════════════════════════════════════════════════

class TestOmieContractCache:
    @pytest.fixture
    def importer(self, tmp_path):
        config = {"omie": {"app_key": "K", "app_secret": "S", "id_conta_corrente": 1,
                           "categoria_ids": {"VISTORIA": "1.01.01"}}}
        return OmieImporter(config, tmp_path)

    def test_cache_vazio_no_inicio(self, importer):
        assert importer._contract_cache == {}

    def test_save_e_load_roundtrip(self, importer, tmp_path):
        importer._contract_cache[3070506708] = 3070506860
        importer._save_contract_cache()
        assert (tmp_path / "omie_contract_cache.json").exists()
        imp2 = OmieImporter(importer.config, tmp_path)
        assert imp2._contract_cache[3070506708] == 3070506860

    def test_prefetch_contratos_popula_cache(self, importer):
        fake = {
            "contratoCadastro": [
                {"cabecalho": {"nCodCli": 100, "nCodCtr": 900, "cCodSit": "10"}},
                {"cabecalho": {"nCodCli": 200, "nCodCtr": 901, "cCodSit": "10"}},
                {"cabecalho": {"nCodCli": 300, "nCodCtr": 902, "cCodSit": "99"}},
            ],
            "total_de_paginas": 1,
        }
        with patch.object(importer.client, "request", return_value=fake):
            total = importer.prefetch_all_contracts()
        assert total == 2
        assert importer._contract_cache[100] == 900
        assert importer._contract_cache[200] == 901
        assert 300 not in importer._contract_cache

    def test_resolve_contract_usa_cache(self, importer):
        importer._contract_cache[555] = 888
        assert importer.resolve_contract(555) == 888

    def test_resolve_contract_sem_contrato_retorna_none(self, importer):
        fake = {"contratoCadastro": [], "total_de_paginas": 1}
        with patch.object(importer.client, "request", return_value=fake):
            assert importer.resolve_contract(999) is None


# ══════════════════════════════════════════════════════════════════════════════
# add_service_to_contract
# ══════════════════════════════════════════════════════════════════════════════

class TestOmieAddServiceToContract:
    @pytest.fixture
    def importer(self, tmp_path):
        config = {"omie": {"app_key": "K", "app_secret": "S", "id_conta_corrente": 1,
                           "categoria_ids": {"VISTORIA CAUTELAR": "1.01.98", "LAUDO": "1.01.02"}}}
        imp = OmieImporter(config, tmp_path)
        from tiny_import import normalize_key
        imp._contact_cache[normalize_key("KAVAK SP MARKET|")] = 3070506708
        imp._contract_cache[3070506708] = 3070506860
        return imp

    def test_add_service_chama_alterar_contrato(self, importer):
        from tiny_import import NormalizedRecord
        rec = NormalizedRecord(
            data="2026-06-15", modelo="GOL", placa="GGB1C74",
            cliente="KAVAK SP MARKET", servico="VISTORIA CAUTELAR",
            fp="FA", preco="150", origem_arquivo="planilha.csv",
            linha_origem=5, chave_deduplicacao="kavak_2026_001",
            av_pagamento="", cpf="", cv="", perito="GIOVANI",
        )
        contrato_existente = {
            "cabecalho": {"nCodCtr": 3070506860, "nCodCli": 3070506708,
                          "cNumCtr": "2026/00001", "dVigInicial": "12/05/2026",
                          "dVigFinal": "30/04/2031", "nDiaFat": 15, "cTipoFat": "01"},
            "itensContrato": [{
                "itemCabecalho": {"seq": 1, "natOperacao": "01", "quant": 1,
                                  "valorUnit": 0, "valorTotal": 0},
                "itemDescrServ": {"descrCompleta": "Serviços prestados"},
            }],
        }
        alterar_resp = {"nCodCtr": 3070506860, "cCodStatus": "0", "cDescStatus": "ok"}
        calls = []
        def mock_request(endpoint, call, param):
            calls.append((endpoint, call))
            if call == "ConsultarContrato":
                return contrato_existente
            if call == "AlterarContrato":
                assert len(param.get("itensContrato", [])) == 2
                novo = param["itensContrato"][1]
                assert novo["itemCabecalho"]["valorUnit"] == 150.0
                assert novo["itemCabecalho"]["seq"] == 2
                assert novo["itemCabecalho"]["codLC116"] == "17.08"
                assert "VISTORIA CAUTELAR" in novo["itemDescrServ"]["descrCompleta"]
                assert "GGB1C74" in novo["itemDescrServ"]["descrCompleta"]
                return alterar_resp
            return {}

        with patch.object(importer.client, "request", side_effect=mock_request):
            resp = importer.add_service_to_contract(rec)
        assert resp["cCodStatus"] == "0"
        assert ("servicos/contrato", "ConsultarContrato") in calls
        assert ("servicos/contrato", "AlterarContrato") in calls

    def test_sem_contrato_levanta_erro(self, importer):
        from tiny_import import NormalizedRecord
        importer._contract_cache.clear()
        rec = NormalizedRecord(
            data="2026-06-15", modelo="", placa="ABC1234",
            cliente="CLIENTE SEM CONTRATO", servico="VISTORIA",
            fp="FA", preco="100", origem_arquivo="test.csv",
            linha_origem=1, chave_deduplicacao="test_001",
            av_pagamento="", cpf="", cv="", perito="",
        )
        from omie_import import OmieApiError
        fake_list = {"contratoCadastro": [], "total_de_paginas": 1}
        def mock_request(endpoint, call, param):
            if call == "ListarContratos":
                return fake_list
            if call == "ConsultarCliente":
                return {"codigo_cliente_omie": 99999}
            if call == "ListarClientes":
                return {"clientes_cadastro": [{"razao_social": "CLIENTE SEM CONTRATO",
                                               "codigo_cliente_omie": 99999}],
                        "total_de_paginas": 1}
            return {}
        with patch.object(importer.client, "request", side_effect=mock_request):
            with pytest.raises(OmieApiError, match="sem contrato ativo"):
                importer.add_service_to_contract(rec)

    def test_categoria_resolvida_pelo_servico(self, importer):
        from tiny_import import NormalizedRecord
        rec = NormalizedRecord(
            data="2026-06-15", modelo="", placa="FKB2E63",
            cliente="KAVAK SP MARKET", servico="LAUDO DE TRANSFERENCIA - ECV",
            fp="FA", preco="100", origem_arquivo="test.csv",
            linha_origem=1, chave_deduplicacao="kavak_laudo_001",
            av_pagamento="", cpf="", cv="", perito="",
        )
        contrato = {
            "cabecalho": {"nCodCtr": 3070506860, "nCodCli": 3070506708,
                          "cNumCtr": "2026/00001", "dVigInicial": "12/05/2026",
                          "dVigFinal": "30/04/2031", "nDiaFat": 15, "cTipoFat": "01"},
            "itensContrato": [{"itemCabecalho": {"seq": 1}}],
        }
        captured_param = {}
        def mock_request(endpoint, call, param):
            if call == "ConsultarContrato":
                return contrato
            if call == "AlterarContrato":
                captured_param.update(param)
                return {"nCodCtr": 3070506860, "cCodStatus": "0"}
            return {}
        with patch.object(importer.client, "request", side_effect=mock_request):
            importer.add_service_to_contract(rec)
        novo_item = captured_param["itensContrato"][1]
        assert novo_item["itemCabecalho"]["cCodCategItem"] == "1.01.02"
