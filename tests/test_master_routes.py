"""
Testes para o painel master: listagem de unidades e status operacional.

Estratégia:
- Usuário master e não-master patcheados via _current_user
- server.UNITS com múltiplas unidades para validar isolamento
- Dados reais em SQLite via fixture de lançamentos
"""
from __future__ import annotations

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

TWO_UNITS = {
    "sp": {"nome": "São Paulo", "master_pin": "0000"},
    "rj": {"nome": "Rio de Janeiro", "master_pin": "1111"},
}

MASTER_USER    = {"email": "admin@astrovistorias.com.br", "name": "Admin", "unit": None, "master": True}
OPERATOR_USER  = {"email": "op@astrovistorias.com.br",    "name": "Op",    "unit": "sp", "master": False}


@pytest.fixture
def master_client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = TWO_UNITS
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=MASTER_USER):
        with server.app.test_client() as c:
            yield c


@pytest.fixture
def operator_client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = TWO_UNITS
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=OPERATOR_USER):
        with server.app.test_client() as c:
            yield c


def _lancar(client, unit, **kwargs):
    payload = {"placa": "ABC1234", "cliente": "TESTE", "servico": "LAUDO", "valor": 100.0, "fp": "pix", **kwargs}
    return client.post(f"/u/{unit}/api/caixa/lancar", json=payload)


# ══════════════════════════════════════════════════════════════════════════════
# /master/api/units
# ══════════════════════════════════════════════════════════════════════════════

class TestMasterUnits:
    def test_lista_todas_unidades(self, master_client):
        body = master_client.get("/master/api/units").get_json()
        ids = {u["id"] for u in body["units"]}
        assert ids == {"sp", "rj"}

    def test_retorna_nome_da_unidade(self, master_client):
        body = master_client.get("/master/api/units").get_json()
        nomes = {u["nome"] for u in body["units"]}
        assert "São Paulo" in nomes

    def test_nao_master_recebe_403(self, operator_client):
        r = operator_client.get("/master/api/units")
        assert r.status_code == 403


# ══════════════════════════════════════════════════════════════════════════════
# /master/api/units/status
# ══════════════════════════════════════════════════════════════════════════════

class TestMasterUnitsStatus:
    def test_retorna_todas_unidades(self, master_client):
        body = master_client.get("/master/api/units/status").get_json()
        ids = {u["id"] for u in body["status"]}
        assert ids == {"sp", "rj"}

    def test_unidade_sem_lancamentos_retorna_zeros(self, master_client):
        body = master_client.get("/master/api/units/status").get_json()
        sp = next(u for u in body["status"] if u["id"] == "sp")
        assert sp["hoje"]["lancamentos"] == 0
        assert sp["hoje"]["total"] == 0.0
        assert sp["hoje"]["ultima_atividade"] is None

    def test_reflete_lancamentos_do_dia(self, master_client):
        _lancar(master_client, "sp", valor=200.0)
        _lancar(master_client, "sp", valor=50.0)
        body = master_client.get("/master/api/units/status").get_json()
        sp = next(u for u in body["status"] if u["id"] == "sp")
        assert sp["hoje"]["lancamentos"] == 2
        assert sp["hoje"]["total"] == 250.0

    def test_isolamento_entre_unidades(self, master_client):
        _lancar(master_client, "sp", valor=300.0)
        body = master_client.get("/master/api/units/status").get_json()
        rj = next(u for u in body["status"] if u["id"] == "rj")
        assert rj["hoje"]["lancamentos"] == 0

    def test_ultima_atividade_preenchida(self, master_client):
        _lancar(master_client, "sp")
        body = master_client.get("/master/api/units/status").get_json()
        sp = next(u for u in body["status"] if u["id"] == "sp")
        assert sp["hoje"]["ultima_atividade"] is not None

    def test_retorna_campo_data(self, master_client):
        body = master_client.get("/master/api/units/status").get_json()
        assert "data" in body

    def test_nao_master_recebe_403(self, operator_client):
        r = operator_client.get("/master/api/units/status")
        assert r.status_code == 403
