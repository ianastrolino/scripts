"""
Testes do endpoint debug /master/api/debug/vistorias-dia.

Read-only, master/matriz only. Quebra a conta de "vistorias do dia" do
painel master em fontes (envios_erp, lancamentos PDV, vistorias_planilha)
pra investigar inflacionamentos (ex: Moema 69 quando esperado eram 45).
"""
from __future__ import annotations

import datetime as dt
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

os.environ.setdefault("SECRET_KEY", "test-secret-key-pytest")
os.environ.setdefault("USERS_CONFIG", "{}")
os.environ.setdefault("UNITS_CONFIG", "{}")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import server  # noqa: E402
from caixa_db import (  # noqa: E402
    insert_envio_tiny, upsert_vistorias_planilha, insert_lancamento,
)


UNITS_FIX = {"sp": {"nome": "São Paulo"}}
MASTER_USER = {"email": "m@a.com", "name": "M", "unit": None, "master": True}
OP_USER     = {"email": "o@a.com", "name": "O", "unit": "sp", "master": False}
HOJE = dt.date.today().isoformat()


@pytest.fixture
def setup(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    yield tmp_path


@pytest.fixture
def master_client(setup):
    with patch.object(server, "_current_user", return_value=MASTER_USER):
        with server.app.test_client() as c:
            yield c


@pytest.fixture
def op_client(setup):
    with patch.object(server, "_current_user", return_value=OP_USER):
        with server.app.test_client() as c:
            yield c


# ══════════════════════════════════════════════════════════════════════════════
# _eh_avulso
# ══════════════════════════════════════════════════════════════════════════════

class TestAvulso:
    @pytest.mark.parametrize("servico,esperado", [
        ("PESQUISA AVULSA",       True),
        ("BAIXA PERMANENTE",      True),
        ("VISTORIA DETRAN",       True),
        ("TAXA DETRAN",           True),
        ("PESQUISA",              True),    # heuristica por palavra-chave
        ("BAIXA PERMANENTE FORA", True),
        ("VISTORIA CAUTELAR",     False),
        ("LAUDO DE TRANSFERENCIA", False),
        ("",                      False),
    ])
    def test_classificacao(self, servico, esperado):
        assert server._eh_avulso(servico) == esperado


# ══════════════════════════════════════════════════════════════════════════════
# Endpoint — caso simples sem dados
# ══════════════════════════════════════════════════════════════════════════════

class TestEndpointVazio:
    def test_unit_invalida_400(self, master_client):
        r = master_client.get("/master/api/debug/vistorias-dia?unit=xxx")
        assert r.status_code == 400

    def test_sem_dados_resumo_zerado(self, master_client):
        r = master_client.get(f"/master/api/debug/vistorias-dia?unit=sp&data={HOJE}")
        body = r.get_json()
        assert body["success"]
        assert body["resumo"]["vistorias_painel"] == 0
        assert body["duplicatas_envios"] == []

    def test_operador_403(self, op_client):
        r = op_client.get(f"/master/api/debug/vistorias-dia?unit=sp&data={HOJE}")
        assert r.status_code == 403


# ══════════════════════════════════════════════════════════════════════════════
# Detector de duplicatas em envios_erp
# ══════════════════════════════════════════════════════════════════════════════

class TestDetectorDuplicatas:
    def _envio(self, key_suffix, **overrides):
        base = {
            "chave_deduplicacao": f"key-{key_suffix}",
            "timestamp":          f"{HOJE}T10:00:00",
            "data_lancamento":    HOJE,
            "placa":              "ABC1234",
            "cliente":            "TESTE",
            "servico":            "VISTORIA CAUTELAR",
            "valor":              100.0,
            "fp":                 "FA",
            "status":             "enviado",
        }
        base.update(overrides)
        return base

    def test_duplicata_aparece(self, master_client, setup):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        # Mesma vistoria enviada 2x com chaves diferentes
        insert_envio_tiny("sp", unit_dir, self._envio("a"))
        insert_envio_tiny("sp", unit_dir, self._envio("b"))

        r = master_client.get(f"/master/api/debug/vistorias-dia?unit=sp&data={HOJE}")
        body = r.get_json()
        assert len(body["duplicatas_envios"]) == 1
        d = body["duplicatas_envios"][0]
        assert d["count"] == 2
        assert d["placa"] == "ABC1234"
        assert body["resumo"]["envios_extras_duplicados"] == 1

    def test_envios_distintos_nao_duplicam(self, master_client, setup):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        insert_envio_tiny("sp", unit_dir, self._envio("1", placa="AAA1111"))
        insert_envio_tiny("sp", unit_dir, self._envio("2", placa="BBB2222"))

        r = master_client.get(f"/master/api/debug/vistorias-dia?unit=sp&data={HOJE}")
        body = r.get_json()
        assert body["duplicatas_envios"] == []
        assert body["resumo"]["envios_extras_duplicados"] == 0


# ══════════════════════════════════════════════════════════════════════════════
# Avulsos do PDV
# ══════════════════════════════════════════════════════════════════════════════

class TestAvulsosPdv:
    def _lc(self, servico, **overrides):
        from datetime import datetime
        ts = f"{HOJE}T11:00:00"
        base = {
            "id":        f"lc-{servico[:3]}",
            "unit":      "sp",
            "data":      HOJE,
            "hora":      "11:00",
            "timestamp": ts,
            "placa":     "ZZZ9999",
            "cliente":   "X",
            "servico":   servico,
            "valor":     50.0,
            "fp":        "dinheiro",
        }
        base.update(overrides)
        return base

    def test_lista_apenas_avulsos(self, master_client, setup):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        insert_lancamento(unit_dir, self._lc("PESQUISA AVULSA",   id="lc-1", placa="A001"))
        insert_lancamento(unit_dir, self._lc("VISTORIA CAUTELAR", id="lc-2", placa="A002"))
        insert_lancamento(unit_dir, self._lc("BAIXA PERMANENTE",  id="lc-3", placa="A003"))

        r = master_client.get(f"/master/api/debug/vistorias-dia?unit=sp&data={HOJE}")
        body = r.get_json()
        servicos = {a["servico"] for a in body["avulsos_pdv"]}
        assert servicos == {"PESQUISA AVULSA", "BAIXA PERMANENTE"}
        assert body["resumo"]["avulsos_pdv"] == 2


# ══════════════════════════════════════════════════════════════════════════════
# Diff PDV/Tiny vs planilha
# ══════════════════════════════════════════════════════════════════════════════

class TestDiffPlanilha:
    def test_no_pdv_sem_planilha(self, master_client, setup):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        # PDV: ABC1234 + ZZZ9999
        from datetime import datetime
        insert_lancamento(unit_dir, {
            "id": "l1", "unit": "sp", "data": HOJE, "hora": "11:00",
            "timestamp": f"{HOJE}T11:00:00", "placa": "ABC1234", "cliente": "X",
            "servico": "VISTORIA CAUTELAR", "valor": 100, "fp": "pix",
        })
        insert_lancamento(unit_dir, {
            "id": "l2", "unit": "sp", "data": HOJE, "hora": "11:01",
            "timestamp": f"{HOJE}T11:01:00", "placa": "ZZZ9999", "cliente": "Y",
            "servico": "PESQUISA AVULSA", "valor": 50, "fp": "dinheiro",
        })
        # Planilha: so ABC1234
        upsert_vistorias_planilha("sp", unit_dir, [
            {"data": HOJE, "placa": "ABC1234", "cliente": "X",
             "servico": "VISTORIA CAUTELAR", "valor": 100, "fp": "AV", "perito": "JOAO"},
        ])

        r = master_client.get(f"/master/api/debug/vistorias-dia?unit=sp&data={HOJE}")
        body = r.get_json()
        assert body["resumo"]["em_pdv_nao_planilha"] == 1
        assert body["resumo"]["em_planilha_nao_pdv"] == 0
        # ZZZ9999 esta no PDV mas nao na planilha (avulso)
        diff = body["diff_pdv_planilha"]["no_pdv_sem_planilha"]
        assert ["ZZZ9999", "PESQUISA AVULSA"] in diff


# ══════════════════════════════════════════════════════════════════════════════
# Pagina HTML
# ══════════════════════════════════════════════════════════════════════════════

class TestPagina:
    def test_html_serve(self, master_client):
        r = master_client.get("/master/debug-vistorias-dia")
        assert r.status_code == 200
        assert b"Anatomia" in r.data

    def test_html_bloqueia_operador(self, op_client):
        r = op_client.get("/master/debug-vistorias-dia")
        assert r.status_code == 403
