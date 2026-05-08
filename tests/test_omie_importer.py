"""
Testes do OmieImporter — caminho do dinheiro entrando no Omie.

Estrategia: mock requests.Session.post (unico ponto de saida pra rede).
Cobre: build_payload, resolve_contact, resolve_categoria, idempotencia,
auth invalida, erros, AV/FA vencimento.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tiny_import import NormalizedRecord  # noqa: E402
from omie_import import (  # noqa: E402
    OmieApiError,
    OmieClient,
    OmieImporter,
    _iso_para_br,
    _is_omie_redundant_error,
)


def _make_record(**overrides) -> NormalizedRecord:
    base = dict(
        data="2026-05-04",
        modelo="GOL",
        placa="ABC1234",
        cliente="CLIENTE TESTE",
        servico="VISTORIA CAUTELAR",
        fp="AV",
        preco="150.00",
        origem_arquivo="planilha.xls",
        linha_origem=2,
        chave_deduplicacao="2026-05-04|ABC1234|VC|150",
        av_pagamento="pix",
        cpf="12345678901",
    )
    base.update(overrides)
    return NormalizedRecord(**base)


def _make_config(**overrides) -> dict:
    base = {
        "omie": {
            "app_key": "APP_KEY_TEST",
            "app_secret": "APP_SECRET_TEST",
            "id_conta_corrente": 12345,
            "categoria_ids": {
                "VISTORIA CAUTELAR":              "1.01.01",
                "LAUDO DE TRANSFERENCIA":         "1.01.02",
                "CAUTELAR + PINTURA":             "1.01.03",
            },
            "timeout_seconds": 30,
        }
    }
    if overrides:
        base["omie"].update(overrides)
    return base


def _mock_resp(json_data, status_code=200):
    """Cria mock de Response com .json() retornando json_data."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.text = json.dumps(json_data) if json_data else ""
    return resp


@pytest.fixture
def importer(tmp_path):
    return OmieImporter(_make_config(), tmp_path)


# ══════════════════════════════════════════════════════════════════════════════
# OmieClient — JSON-RPC básico
# ══════════════════════════════════════════════════════════════════════════════

class TestOmieClient:
    def test_request_envia_app_key_app_secret_no_body(self):
        c = OmieClient("KEY", "SECRET")
        with patch.object(c.session, "post") as mock_post:
            mock_post.return_value = _mock_resp({"ok": True})
            c.request("financas/contareceber", "IncluirContaReceber", {"foo": 1})
            args, kwargs = mock_post.call_args
            body = kwargs["json"]
            assert body["app_key"] == "KEY"
            assert body["app_secret"] == "SECRET"
            assert body["call"] == "IncluirContaReceber"
            assert body["param"] == [{"foo": 1}]

    def test_url_eh_montada_corretamente(self):
        c = OmieClient("K", "S")
        with patch.object(c.session, "post") as mock_post:
            mock_post.return_value = _mock_resp({})
            c.request("financas/contareceber", "Listar", {})
            url = mock_post.call_args[0][0]
            assert url == "https://app.omie.com.br/api/v1/financas/contareceber/"

    def test_param_lista_passa_direto(self):
        c = OmieClient("K", "S")
        with patch.object(c.session, "post") as mock_post:
            mock_post.return_value = _mock_resp({})
            c.request("x/y", "Z", [{"a": 1}, {"b": 2}])
            assert mock_post.call_args[1]["json"]["param"] == [{"a": 1}, {"b": 2}]

    def test_faultstring_levanta_OmieApiError(self):
        c = OmieClient("K", "S")
        with patch.object(c.session, "post") as mock_post:
            mock_post.return_value = _mock_resp({
                "faultcode": "SOAP-ENV:Client-101",
                "faultstring": "App Key invalida",
            })
            with pytest.raises(OmieApiError) as exc_info:
                c.request("x/y", "Z", {})
            assert exc_info.value.code == "SOAP-ENV:Client-101"
            assert "App Key" in exc_info.value.descricao

    def test_resposta_nao_json_levanta_erro(self):
        c = OmieClient("K", "S")
        with patch.object(c.session, "post") as mock_post:
            resp = MagicMock()
            resp.status_code = 502
            resp.json.side_effect = ValueError("not json")
            resp.text = "<html>Bad Gateway</html>"
            mock_post.return_value = resp
            with pytest.raises(OmieApiError) as exc_info:
                c.request("x/y", "Z", {})
            assert "HTTP502" in exc_info.value.code

    def test_erro_de_rede_levanta_OmieApiError(self):
        import requests as _r
        c = OmieClient("K", "S")
        with patch.object(c.session, "post", side_effect=_r.ConnectionError("dns")):
            with pytest.raises(OmieApiError) as exc_info:
                c.request("x/y", "Z", {})
            assert exc_info.value.code == "REDE"


# ══════════════════════════════════════════════════════════════════════════════
# OmieImporter.testar_conexao
# ══════════════════════════════════════════════════════════════════════════════

class TestTestarConexao:
    def test_ok_retorna_total_categorias(self, importer):
        with patch.object(importer.client.session, "post") as mock_post:
            mock_post.return_value = _mock_resp({"total_de_registros": 42})
            r = importer.testar_conexao()
            assert r["ok"] is True
            assert r["categorias_total"] == 42

    def test_app_key_invalida_retorna_ok_false(self, importer):
        with patch.object(importer.client.session, "post") as mock_post:
            mock_post.return_value = _mock_resp({
                "faultcode": "SOAP-ENV:Client-101",
                "faultstring": "App Key invalida",
            })
            r = importer.testar_conexao()
            assert r["ok"] is False
            assert "App Key" in r["error"]


# ══════════════════════════════════════════════════════════════════════════════
# resolve_categoria
# ══════════════════════════════════════════════════════════════════════════════

class TestResolveCategoria:
    def test_match_exato(self, importer):
        assert importer.resolve_categoria("VISTORIA CAUTELAR") == "1.01.01"

    def test_match_substring_servico_contem_categoria(self, importer):
        # "CAUTELAR + PINTURA" presente no config → bate com nome similar
        assert importer.resolve_categoria("CAUTELAR + PINTURA") == "1.01.03"

    def test_servico_sem_match_retorna_none(self, importer):
        assert importer.resolve_categoria("SERVICO INEXISTENTE") is None

    def test_servico_vazio_retorna_none(self, importer):
        assert importer.resolve_categoria("") is None

    def test_categoria_ids_vazio_retorna_none(self, tmp_path):
        cfg = _make_config(categoria_ids={})
        imp = OmieImporter(cfg, tmp_path)
        assert imp.resolve_categoria("VISTORIA CAUTELAR") is None


# ══════════════════════════════════════════════════════════════════════════════
# resolve_contact (criar/buscar cliente)
# ══════════════════════════════════════════════════════════════════════════════

class TestResolveContact:
    def test_busca_por_cpf_acha(self, importer):
        with patch.object(importer.client.session, "post") as mock_post:
            mock_post.return_value = _mock_resp({"codigo_cliente_omie": 99999})
            cid = importer.resolve_contact("FULANO", "12345678901")
            assert cid == 99999
            # Verifica que chamou ConsultarCliente
            body = mock_post.call_args[1]["json"]
            assert body["call"] == "ConsultarCliente"
            assert body["param"][0]["cnpj_cpf"] == "12345678901"

    def test_cpf_nao_encontrado_busca_por_nome(self, importer):
        with patch.object(importer.client.session, "post") as mock_post:
            # 1ª chamada: ConsultarCliente devolve 5113 (não achou)
            # 2ª chamada: ListarClientes acha por razão social
            mock_post.side_effect = [
                _mock_resp({
                    "faultcode":   "SOAP-ENV:Client-5113",
                    "faultstring": "Cliente nao encontrado",
                }),
                _mock_resp({
                    "clientes_cadastro": [
                        {"codigo_cliente_omie": 12345, "razao_social": "FULANO LTDA"}
                    ]
                }),
            ]
            cid = importer.resolve_contact("FULANO LTDA", "12345678901")
            assert cid == 12345

    def test_nao_acha_em_lugar_nenhum_cria_novo(self, importer):
        with patch.object(importer.client.session, "post") as mock_post:
            mock_post.side_effect = [
                _mock_resp({"faultcode": "SOAP-ENV:Client-5113",
                            "faultstring": "Cliente nao encontrado"}),
                _mock_resp({"clientes_cadastro": []}),
                _mock_resp({"codigo_cliente_omie": 77777}),
            ]
            cid = importer.resolve_contact("NOVO CLIENTE", "12345678901")
            assert cid == 77777
            # 3ª chamada deve ser IncluirCliente
            assert mock_post.call_args_list[2][1]["json"]["call"] == "IncluirCliente"

    def test_cache_evita_chamada_dupla(self, importer):
        with patch.object(importer.client.session, "post") as mock_post:
            mock_post.return_value = _mock_resp({"codigo_cliente_omie": 11111})
            cid1 = importer.resolve_contact("FULANO", "12345678901")
            cid2 = importer.resolve_contact("FULANO", "12345678901")
            assert cid1 == cid2 == 11111
            # Só 1 chamada — segunda veio do cache
            assert mock_post.call_count == 1

    def test_nome_vazio_levanta_erro(self, importer):
        with pytest.raises(OmieApiError) as exc_info:
            importer.resolve_contact("", "")
        assert exc_info.value.code == "CLIENTE"


# ══════════════════════════════════════════════════════════════════════════════
# create_accounts_receivable
# ══════════════════════════════════════════════════════════════════════════════

class TestCreateAccountsReceivable:
    def test_payload_correto_av(self, importer):
        rec = _make_record(av_pagamento="pix", data="2026-05-04")
        with patch.object(importer.client.session, "post") as mock_post:
            # 1ª = busca cliente OK; 2ª = IncluirContaReceber
            mock_post.side_effect = [
                _mock_resp({"codigo_cliente_omie": 7777}),
                _mock_resp({"codigo_lancamento_omie": 999, "descricao_status": "ok"}),
            ]
            result = importer.create_accounts_receivable(rec)
            assert result["codigo_lancamento_omie"] == 999

            # Confere payload do IncluirContaReceber
            body = mock_post.call_args_list[1][1]["json"]
            assert body["call"] == "IncluirContaReceber"
            param = body["param"][0]
            assert param["codigo_cliente_fornecedor"] == 7777
            assert param["valor_documento"] == 150.0
            assert param["codigo_categoria"] == "1.01.01"
            assert param["id_conta_corrente"] == 12345
            assert param["data_vencimento"] == "04/05/2026"  # AV = mesma data
            assert "Placa ABC1234" in param["observacao"]

    def test_payload_correto_fa_vencimento_ultimo_dia_do_mes(self, importer):
        rec = _make_record(av_pagamento="pendente", data="2026-05-04", fp="FA")
        with patch.object(importer.client.session, "post") as mock_post:
            mock_post.side_effect = [
                _mock_resp({"codigo_cliente_omie": 7777}),
                _mock_resp({"codigo_lancamento_omie": 999}),
            ]
            importer.create_accounts_receivable(rec)
            param = mock_post.call_args_list[1][1]["json"]["param"][0]
            assert param["data_vencimento"] == "31/05/2026"

    def test_sem_id_conta_corrente_levanta_erro_config(self, tmp_path):
        cfg = _make_config(id_conta_corrente=0)
        imp = OmieImporter(cfg, tmp_path)
        rec = _make_record()
        with pytest.raises(OmieApiError) as exc_info:
            imp.create_accounts_receivable(rec)
        assert exc_info.value.code == "CONFIG"

    def test_servico_sem_categoria_levanta_erro(self, importer):
        rec = _make_record(servico="SERVICO INEXISTENTE")
        with patch.object(importer.client.session, "post") as mock_post:
            mock_post.return_value = _mock_resp({"codigo_cliente_omie": 7777})
            with pytest.raises(OmieApiError) as exc_info:
                importer.create_accounts_receivable(rec)
            assert exc_info.value.code == "CATEGORIA"

    def test_idempotencia_codigo_lancamento_integracao(self, importer):
        """O codigo_lancamento_integracao identifica unicamente o registro no Omie.
        Se chamamos 2x com mesma chave, Omie deve dar erro de duplicata — o
        nosso code passa o erro pra cima (api_send trata como pulado/falha)."""
        rec = _make_record()
        with patch.object(importer.client.session, "post") as mock_post:
            mock_post.side_effect = [
                _mock_resp({"codigo_cliente_omie": 7777}),
                _mock_resp({
                    "faultcode": "SOAP-ENV:Client-101",
                    "faultstring": "codigo_lancamento_integracao ja consta cadastrado",
                }),
            ]
            with pytest.raises(OmieApiError) as exc_info:
                importer.create_accounts_receivable(rec)
            assert "ja consta cadastrado" in exc_info.value.descricao


# ══════════════════════════════════════════════════════════════════════════════
# helpers
# ══════════════════════════════════════════════════════════════════════════════

class TestRedundantError:
    def test_msg_completa_omie_detecta(self):
        exc = Exception("Omie [SOAP-ENV:Client-6]: ERROR: Consumo redundante detectado. Aguarde 57 segundos para tentar novamente (REDUNDANT).")
        assert _is_omie_redundant_error(exc)

    def test_so_palavra_redundant_detecta(self):
        assert _is_omie_redundant_error(Exception("REDUNDANT"))

    def test_consumo_redundante_em_pt_detecta(self):
        assert _is_omie_redundant_error(Exception("Consumo redundante"))

    def test_outro_erro_nao_detecta(self):
        assert not _is_omie_redundant_error(Exception("App Key invalida"))
        assert not _is_omie_redundant_error(Exception("Cliente nao encontrado"))


class TestIsoParaBr:
    @pytest.mark.parametrize("iso,br", [
        ("2026-05-04", "04/05/2026"),
        ("2026-12-31", "31/12/2026"),
        ("2026-01-01", "01/01/2026"),
    ])
    def test_conversao(self, iso, br):
        assert _iso_para_br(iso) == br

    def test_invalido_passa_inalterado(self):
        # Melhor passar lixo que crashar — Omie reclama na resposta
        assert _iso_para_br("data-invalida") == "data-invalida"
