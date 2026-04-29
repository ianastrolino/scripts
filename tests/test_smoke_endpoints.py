"""
Smoke test: bate em endpoints GET conhecidos como master e verifica que NENHUM
retorna 500 ou demora mais que um threshold.

Roda no CI pra pegar regressao de:
- Endpoint que comecou a estourar exception (500)
- Endpoint que comecou a fazer sync com Tiny/Omie sem timeout
- Refator que renomeou rota e quebrou silenciosamente

Mock do Tiny: nao queremos bater na rede no CI.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

os.environ.setdefault("SECRET_KEY", "test-secret-key-pytest")
os.environ.setdefault("USERS_CONFIG", "{}")
os.environ.setdefault("UNITS_CONFIG", "{}")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import server  # noqa: E402

MASTER_USER = {"email": "admin@astro.com", "name": "Admin", "unit": None, "master": True}

UNITS_FIX = {
    "sp": {"nome": "São Paulo", "master_pin": "0000", "erp": "tiny"},
    "rj": {"nome": "Rio de Janeiro", "master_pin": "1111", "erp": "omie"},
}

# Endpoints master (sem placeholder de unit)
MASTER_ENDPOINTS = [
    "/health",
    "/api/me",
    "/api/csrf-token",
    "/master/api/units",
    "/master/api/units/status",
    "/master/api/visao-geral",
    "/master/api/sistema/saude",
    "/master/api/diag/tokens",
    "/master/api/tiny-health",
    "/master/api/duplicados-envios",
    "/master/api/usuarios-conectados",
    "/master/api/contas-receber",
    "/master/api/royalties",
    "/master/api/aprovacoes",
    "/master/api/auditoria",
    "/master/api/usuarios",
    "/master/api/convites",
    "/master/api/unidades",
    "/master/api/roadmap",
    "/master/api/backup/status",
    "/master/api/debug/storage",
    "/master/api/js-errors",
]

# Endpoints por unit — sao testados pra cada uid em UNITS_FIX
UNIT_ENDPOINTS = [
    "/u/{u}/api/info",
    "/u/{u}/api/caixa/estado",
    "/u/{u}/api/planilha/status",
    "/u/{u}/api/planilha/dia",
    "/u/{u}/api/fechamento/relatorio",
]

SLOW_THRESHOLD_S = 2.0  # qualquer endpoint acima disso falha o teste


@pytest.fixture
def master_client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    # Mock funcoes que batem em rede externa (Tiny). Retornam dados vazios mas
    # validos pro endpoint nao explodir.
    with patch.object(server, "_fetch_contas_mes_tiny", return_value=[]), \
         patch.object(server, "_fetch_contas_abertas_tiny", return_value=[]), \
         patch.object(server, "_current_user", return_value=MASTER_USER):
        with server.app.test_client() as c:
            yield c


def _all_endpoints():
    yield from MASTER_ENDPOINTS
    for uid in UNITS_FIX:
        for tpl in UNIT_ENDPOINTS:
            yield tpl.format(u=uid)


@pytest.mark.parametrize("path", list(_all_endpoints()))
def test_endpoint_nao_retorna_500(master_client, path):
    """Regressao: nenhum endpoint conhecido deve retornar 500."""
    r = master_client.get(path)
    assert r.status_code != 500, f"{path} retornou 500: {r.get_data(as_text=True)[:300]}"
    # 200/401/403/404 sao todos aceitaveis pra smoke; 500 nao.


@pytest.mark.parametrize("path", list(_all_endpoints()))
def test_endpoint_responde_rapido(master_client, path):
    """Regressao: nenhum endpoint deve passar do threshold de latencia."""
    t = time.monotonic()
    master_client.get(path)
    elapsed = time.monotonic() - t
    assert elapsed < SLOW_THRESHOLD_S, f"{path} levou {elapsed:.2f}s (threshold {SLOW_THRESHOLD_S}s)"
