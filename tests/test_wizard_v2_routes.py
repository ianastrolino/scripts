"""
Testes de integracao para os endpoints novos do Wizard de Fechamento v2 e
da Conferencia Antecipada (commits 6f51413 -> 660be1b).

Cobertura:
- POST /api/fechamento/decisao  — persiste decisao manual
- GET  /api/fechamento/relatorio — agregado por tipo
- POST /api/planilha/upload     — persiste planilha do dia
- GET  /api/planilha/dia        — devolve planilha
- GET  /api/planilha/status     — cruza com PDV em tempo real

Estrategia: Flask test client, DATA_DIR em tmp_path, sem rede.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Data "hoje" do PDV — usada nos tests pra evitar dependencia de data
# do calendario real. Tests que cruzam planilha+PDV precisam usar a
# mesma data, e _lancar grava no PDV do dia atual real.
HOJE = dt.date.today().isoformat()
ONTEM = (dt.date.today() - dt.timedelta(days=1)).isoformat()

os.environ.setdefault("SECRET_KEY", "test-secret-key-pytest")
os.environ.setdefault("USERS_CONFIG", "{}")
os.environ.setdefault("UNITS_CONFIG", '{"testunit": {"nome": "Test Unit", "master_pin": "1234"}}')

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import server  # noqa: E402

UNIT = "testunit"
FAKE_USER = {"email": "op@astrovistorias.com.br", "name": "Operador", "unit": UNIT, "master": False}


@pytest.fixture
def client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = {"testunit": {"nome": "Test Unit", "master_pin": "1234"}}
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=FAKE_USER):
        with server.app.test_client() as c:
            yield c


def _lancar(client, **kwargs):
    payload = {"placa": "ABC1234", "cliente": "TESTE", "servico": "LAUDO", "valor": 150.0, "fp": "pix", **kwargs}
    return client.post(f"/u/{UNIT}/api/caixa/lancar", json=payload)


# ══════════════════════════════════════════════════════════════════════════════
# /api/fechamento/decisao — persistencia
# ══════════════════════════════════════════════════════════════════════════════

class TestDecisao:
    def _post(self, client, **body):
        body.setdefault("data", "2026-04-26")
        body.setdefault("alvo", {})
        return client.post(f"/u/{UNIT}/api/fechamento/decisao", json=body)

    def test_data_invalida_400(self, client):
        r = self._post(client, data="invalida", tipo="marcar_ignorar")
        assert r.status_code == 400
        assert "data invalida" in r.get_json()["error"].lower()

    def test_tipo_invalido_400(self, client):
        r = self._post(client, tipo="nao_existe")
        assert r.status_code == 400
        assert "tipo invalido" in r.get_json()["error"].lower()

    def test_marcar_ignorar_sem_motivo_passa(self, client):
        r = self._post(client, tipo="marcar_ignorar", alvo={"rid": "x"})
        assert r.status_code == 200
        body = r.get_json()
        assert body["success"] is True
        assert body["decisao"]["tipo"] == "marcar_ignorar"
        assert body["total_decisoes_dia"] == 1

    def test_cortesia_sem_motivo_400(self, client):
        r = self._post(client, tipo="marcar_cortesia", alvo={"rid": "x"}, pin="1234")
        assert r.status_code == 400
        assert "motivo obrigatorio" in r.get_json()["error"].lower()

    def test_cortesia_sem_pin_403_pin_required(self, client):
        r = self._post(client, tipo="marcar_cortesia", alvo={"rid": "x"}, motivo="cliente VIP")
        assert r.status_code == 403
        body = r.get_json()
        assert body["code"] == "pin_required"

    def test_cortesia_pin_invalido_403_pin_invalid(self, client):
        r = self._post(client, tipo="marcar_cortesia", alvo={"rid": "x"}, motivo="x", pin="9999")
        assert r.status_code == 403
        body = r.get_json()
        assert body["code"] == "pin_invalid"

    def test_cortesia_pin_correto_passa(self, client):
        r = self._post(client, tipo="marcar_cortesia", alvo={"rid": "x"}, motivo="cliente VIP", pin="1234")
        assert r.status_code == 200
        body = r.get_json()
        assert body["success"] is True
        assert body["decisao"]["pin_ok"] is True
        assert body["decisao"]["motivo"] == "cliente VIP"

    def test_faturado_sem_pin_passa_com_motivo(self, client):
        # Regra Ian: marcar_faturado nao exige PIN, so motivo
        r = self._post(client, tipo="marcar_faturado", alvo={"rid": "x"}, motivo="cliente pagara depois")
        assert r.status_code == 200
        body = r.get_json()
        assert body["decisao"]["pin_ok"] is False

    def test_adicionar_pagamento_exige_forma_pagamento(self, client):
        r = self._post(client, tipo="adicionar_pagamento", alvo={"rid": "x"})
        assert r.status_code == 400
        assert "forma_pagamento" in r.get_json()["error"].lower()

    def test_adicionar_pagamento_fp_invalida_400(self, client):
        r = self._post(client, tipo="adicionar_pagamento", alvo={"rid": "x", "forma_pagamento": "boleto"})
        assert r.status_code == 400

    def test_adicionar_pagamento_fp_valida_passa(self, client):
        r = self._post(client, tipo="adicionar_pagamento", alvo={"rid": "x", "forma_pagamento": "pix"})
        assert r.status_code == 200
        body = r.get_json()
        assert body["decisao"]["alvo"]["forma_pagamento"] == "pix"

    def test_decisao_acumula_no_dia(self, client):
        for i in range(3):
            self._post(client, tipo="marcar_ignorar", alvo={"rid": f"r{i}"})
        r = self._post(client, tipo="marcar_ignorar", alvo={"rid": "r3"})
        assert r.get_json()["total_decisoes_dia"] == 4


# ══════════════════════════════════════════════════════════════════════════════
# /api/fechamento/relatorio — agregado
# ══════════════════════════════════════════════════════════════════════════════

class TestRelatorio:
    def _decidir(self, client, tipo, **extra):
        body = {"data": "2026-04-26", "tipo": tipo, "alvo": {"rid": "x"}, **extra}
        return client.post(f"/u/{UNIT}/api/fechamento/decisao", json=body)

    def test_relatorio_vazio(self, client):
        r = client.get(f"/u/{UNIT}/api/fechamento/relatorio?data=2026-04-26")
        body = r.get_json()
        assert body["success"] is True
        assert body["total_decisoes"] == 0
        assert body["por_tipo"] == {}

    def test_default_data_eh_hoje(self, client):
        r = client.get(f"/u/{UNIT}/api/fechamento/relatorio")
        assert r.get_json()["success"] is True

    def test_agrega_por_tipo(self, client):
        self._decidir(client, "marcar_ignorar")
        self._decidir(client, "marcar_ignorar")
        self._decidir(client, "marcar_faturado", motivo="x")
        r = client.get(f"/u/{UNIT}/api/fechamento/relatorio?data=2026-04-26")
        body = r.get_json()
        assert body["total_decisoes"] == 3
        assert body["por_tipo"]["marcar_ignorar"] == 2
        assert body["por_tipo"]["marcar_faturado"] == 1


# ══════════════════════════════════════════════════════════════════════════════
# /api/planilha/upload + dia + status — Conferencia Antecipada
# ══════════════════════════════════════════════════════════════════════════════

class TestPlanilhaDia:
    def _records(self, **overrides):
        base = [
            {"id": "p1", "placa": "ABC1234", "servico": "CAUTELAR",     "preco": 80.0, "fp": "AV", "data": "2026-04-26"},
            {"id": "p2", "placa": "XYZ9999", "servico": "TRANSFERENCIA", "preco": 120.0, "fp": "AV", "data": "2026-04-26"},
        ]
        return overrides.get("records", base)

    def _upload(self, client, **body):
        body.setdefault("data", "2026-04-26")
        body.setdefault("records", self._records())
        body.setdefault("arquivo", "test.xls")
        return client.post(f"/u/{UNIT}/api/planilha/upload", json=body)

    def test_upload_data_invalida_400(self, client):
        r = self._upload(client, data="invalido")
        assert r.status_code == 400

    def test_upload_records_nao_lista_400(self, client):
        r = self._upload(client, records="nao-eh-lista")
        assert r.status_code == 400

    def test_upload_persiste_e_versiona(self, client):
        r = self._upload(client)
        body = r.get_json()
        assert body["success"] is True
        assert body["total"] == 2
        assert body["versao"] == 1
        assert body["placas_removidas"] == []

        # 2o upload incrementa versao
        r2 = self._upload(client)
        assert r2.get_json()["versao"] == 2

    def test_diff_detecta_placa_removida(self, client):
        self._upload(client)
        # Re-upload sem XYZ9999
        r = self._upload(client, records=[
            {"id": "p1", "placa": "ABC1234", "servico": "CAUTELAR", "preco": 80.0, "fp": "AV", "data": "2026-04-26"}
        ])
        assert "XYZ9999" in r.get_json()["placas_removidas"]

    def test_get_dia_inexistente(self, client):
        r = client.get(f"/u/{UNIT}/api/planilha/dia?data=2026-04-26")
        body = r.get_json()
        assert body["exists"] is False
        assert body["records"] == []

    def test_get_dia_apos_upload(self, client):
        self._upload(client)
        r = client.get(f"/u/{UNIT}/api/planilha/dia?data=2026-04-26")
        body = r.get_json()
        assert body["exists"] is True
        assert len(body["records"]) == 2
        assert body["versao"] == 1


class TestPlanilhaStatus:
    def _upload(self, client, records, data_iso="2026-04-26"):
        return client.post(f"/u/{UNIT}/api/planilha/upload", json={
            "data": data_iso, "arquivo": "test.xls", "records": records
        })

    def test_status_sem_planilha(self, client):
        r = client.get(f"/u/{UNIT}/api/planilha/status?data=2026-04-26")
        body = r.get_json()
        assert body["success"] is True
        assert body["exists"] is False
        assert body["stats"]["total"] == 0

    def test_status_planilha_sem_pdv(self, client):
        # Planilha tem 2, PDV vazio → 2 orfas planilha
        self._upload(client, [
            {"id": "p1", "placa": "ABC1234", "servico": "CAUTELAR", "preco": 80.0, "fp": "AV", "data": "2026-04-26"},
            {"id": "p2", "placa": "XYZ9999", "servico": "TRANSFERENCIA", "preco": 120.0, "fp": "AV", "data": "2026-04-26"},
        ])
        r = client.get(f"/u/{UNIT}/api/planilha/status?data=2026-04-26")
        body = r.get_json()
        assert body["exists"] is True
        assert body["stats"]["total"] == 2
        assert body["stats"]["cruzadas"] == 0
        assert body["stats"]["orfas_planilha"] == 2

    def test_status_match_exato_quando_pdv_existe(self, client):
        _lancar(client, placa="ABC1234", servico="CAUTELAR", valor=80.0, fp="dinheiro")
        self._upload(client, [
            {"id": "p1", "placa": "ABC1234", "servico": "CAUTELAR", "preco": 80.0, "fp": "AV", "data": HOJE},
        ], data_iso=HOJE)
        r = client.get(f"/u/{UNIT}/api/planilha/status?data={HOJE}")
        body = r.get_json()
        assert body["stats"]["cruzadas"] == 1
        assert body["stats"]["orfas_planilha"] == 0
        linha = body["linhas"][0]
        assert linha["status"] == "ok"
        assert linha["pdv_match"] is not None
        assert linha["dia_anterior"] is False

    def test_status_dia_anterior_eh_flagado(self, client):
        # Vistoria com data diferente da query (ontem na planilha, hoje na query)
        self._upload(client, [
            {"id": "p1", "placa": "ABC1234", "servico": "CAUTELAR", "preco": 80.0, "fp": "AV", "data": ONTEM},
        ], data_iso=HOJE)
        r = client.get(f"/u/{UNIT}/api/planilha/status?data={HOJE}")
        body = r.get_json()
        assert body["stats"]["dia_anterior"] == 1
        assert body["linhas"][0]["dia_anterior"] is True

    def test_status_orfa_pdv_quando_lancamento_sem_planilha(self, client):
        _lancar(client, placa="ZZZ0001", servico="PESQUISA AVULSA", valor=50.0, fp="dinheiro")
        # Nao faz upload de planilha
        r = client.get(f"/u/{UNIT}/api/planilha/status?data={HOJE}")
        body = r.get_json()
        # exists=False mas orfas_pdv ainda eh stats={}
        assert body["exists"] is False

    def test_status_divergencia_valor_detectada(self, client):
        _lancar(client, placa="ABC1234", servico="CAUTELAR", valor=100.0, fp="pix")
        self._upload(client, [
            {"id": "p1", "placa": "ABC1234", "servico": "CAUTELAR", "preco": 150.0, "fp": "AV", "data": HOJE},
        ], data_iso=HOJE)
        r = client.get(f"/u/{UNIT}/api/planilha/status?data={HOJE}")
        body = r.get_json()
        # placa bate mas valor difere → divergencia
        assert body["stats"]["divergencias"] == 1
        assert body["linhas"][0]["status"] == "divergencia_valor"
