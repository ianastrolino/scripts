"""
Testes do endpoint /master/api/backfill-vistorias.

Usado pra alimentar vistorias_planilha em massa com planilhas historicas.
Master/matriz so — popula a tabela diretamente sem passar pelo fluxo de
fechamento (snapshot, conferencia, envio).
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
from caixa_db import load_vistorias_planilha  # noqa: E402


UNITS_FIX = {"sp": {"nome": "São Paulo"}, "rj": {"nome": "Rio de Janeiro"}}
MASTER_USER = {"email": "m@a.com", "name": "M", "unit": None, "master": True}
MATRIZ_USER = {"email": "x@a.com", "name": "X", "unit": None, "master": False, "matriz": True}
OP_USER     = {"email": "o@a.com", "name": "O", "unit": "sp", "master": False}


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
def matriz_client(setup):
    with patch.object(server, "_current_user", return_value=MATRIZ_USER):
        with server.app.test_client() as c:
            yield c


@pytest.fixture
def op_client(setup):
    with patch.object(server, "_current_user", return_value=OP_USER):
        with server.app.test_client() as c:
            yield c


def _vistorias_payload(unit="sp", n=3, **overrides):
    base = [
        {"data": "2026-05-01", "placa": f"A{i:04d}", "cliente": "X",
         "servico": "VISTORIA CAUTELAR", "valor": 100, "fp": "AV",
         "perito": "JOAO"}
        for i in range(n)
    ]
    for v in base:
        v.update(overrides)
    return {"unit": unit, "arquivo": "maio.xls", "vistorias": base}


# ══════════════════════════════════════════════════════════════════════════════
# Caso feliz
# ══════════════════════════════════════════════════════════════════════════════

class TestBackfillBasico:
    def test_insere_em_unidade(self, master_client, setup):
        r = master_client.post("/master/api/backfill-vistorias",
                               json=_vistorias_payload(n=5))
        body = r.get_json()
        assert r.status_code == 200
        assert body["success"]
        assert body["inseridas"] == 5
        assert body["atualizadas"] == 0

        unit_dir = server._unit_state_dir("sp")
        rows = load_vistorias_planilha("sp", unit_dir, "2026-05-01", "2026-05-31")
        assert len(rows) == 5
        assert rows[0]["perito"] == "JOAO"
        assert rows[0]["arquivo"] == "maio.xls"

    def test_reimport_atualiza_nao_duplica(self, master_client, setup):
        master_client.post("/master/api/backfill-vistorias", json=_vistorias_payload(n=3))
        # Reimport
        r = master_client.post("/master/api/backfill-vistorias", json=_vistorias_payload(n=3))
        body = r.get_json()
        assert body["inseridas"] == 0
        assert body["atualizadas"] == 3
        unit_dir = server._unit_state_dir("sp")
        rows = load_vistorias_planilha("sp", unit_dir, "2026-05-01", "2026-05-31")
        assert len(rows) == 3  # Sem duplicata

    def test_aplica_alias_de_servico(self, master_client, setup):
        """Servicos com alias configurado devem ser normalizados."""
        # Default config nao tem aliases — so confere que servico chega normalizado
        r = master_client.post("/master/api/backfill-vistorias",
                               json=_vistorias_payload(n=1, servico="vistoria cautelar"))
        assert r.status_code == 200
        unit_dir = server._unit_state_dir("sp")
        rows = load_vistorias_planilha("sp", unit_dir, "2026-05-01", "2026-05-31")
        assert rows[0]["servico"] == "VISTORIA CAUTELAR"

    def test_unidades_separadas(self, master_client, setup):
        master_client.post("/master/api/backfill-vistorias",
                           json=_vistorias_payload(unit="sp", n=3))
        master_client.post("/master/api/backfill-vistorias",
                           json=_vistorias_payload(unit="rj", n=2))
        sp_rows = load_vistorias_planilha("sp", server._unit_state_dir("sp"),
                                          "2026-05-01", "2026-05-31")
        rj_rows = load_vistorias_planilha("rj", server._unit_state_dir("rj"),
                                          "2026-05-01", "2026-05-31")
        assert len(sp_rows) == 3
        assert len(rj_rows) == 2


# ══════════════════════════════════════════════════════════════════════════════
# Validacao
# ══════════════════════════════════════════════════════════════════════════════

class TestValidacao:
    def test_unidade_inexistente_400(self, master_client):
        r = master_client.post("/master/api/backfill-vistorias",
                               json={"unit": "xx", "vistorias": [{"data": "2026-05-01"}]})
        assert r.status_code == 400

    def test_vistorias_vazio_400(self, master_client):
        r = master_client.post("/master/api/backfill-vistorias",
                               json={"unit": "sp", "vistorias": []})
        assert r.status_code == 400


# ══════════════════════════════════════════════════════════════════════════════
# Permissao
# ══════════════════════════════════════════════════════════════════════════════

class TestPermissao:
    def test_master_pode(self, master_client, setup):
        r = master_client.post("/master/api/backfill-vistorias", json=_vistorias_payload())
        assert r.status_code == 200

    def test_matriz_pode(self, matriz_client, setup):
        r = matriz_client.post("/master/api/backfill-vistorias", json=_vistorias_payload())
        assert r.status_code == 200

    def test_operador_403(self, op_client, setup):
        r = op_client.post("/master/api/backfill-vistorias", json=_vistorias_payload())
        assert r.status_code == 403


# ══════════════════════════════════════════════════════════════════════════════
# Pagina HTML
# ══════════════════════════════════════════════════════════════════════════════

class TestPagina:
    def test_html_serve(self, master_client):
        r = master_client.get("/master/backfill-vistoriadores")
        assert r.status_code == 200
        assert b"Backfill" in r.data

    def test_html_bloqueia_operador(self, op_client):
        r = op_client.get("/master/backfill-vistoriadores")
        assert r.status_code == 403
