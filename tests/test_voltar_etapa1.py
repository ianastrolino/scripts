"""
Testes do endpoint /u/<unit>/api/fechamento/voltar-para-lancamentos.

Cenario: operadora avancou sem querer pra etapa 2 (Conferencia iniciada)
e ficou travada pra novos lancamentos. Endpoint reverte pra etapa 1.

Cobre:
- _voltar_para_lancamentos remove conferencia_iniciada_em
- Endpoint retorna 200 + etapa=1
- Etapa 3 (fechado) NAO eh afetada (precisa _reabrir_dia)
- Lancamento em etapa 2 nao exige mais PIN (so etapa 3)
- Audit log gravado
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

UNIT = "sp"
UNITS_FIX = {"sp": {"nome": "São Paulo", "master_pin": "0000"}}
USER = {"email": "op@astro.com", "name": "Op", "unit": "sp", "master": False}
HOJE = dt.date.today().isoformat()


@pytest.fixture
def client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=USER):
        with server.app.test_client() as c:
            yield c


# ══════════════════════════════════════════════════════════════════════════════
# _voltar_para_lancamentos (helper puro)
# ══════════════════════════════════════════════════════════════════════════════

class TestVoltarHelper:
    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path):
        server.DATA_DIR = tmp_path
        server.UNITS = UNITS_FIX

    def test_etapa_2_volta_pra_1(self):
        server._iniciar_conferencia(UNIT, HOJE, "op@astro.com")
        assert server._dia_etapa(UNIT, HOJE) == 2
        ok = server._voltar_para_lancamentos(UNIT, HOJE, "op@astro.com")
        assert ok is True
        assert server._dia_etapa(UNIT, HOJE) == 1

    def test_etapa_1_retorna_false(self):
        # Nao tem conferencia iniciada — nada a voltar
        ok = server._voltar_para_lancamentos(UNIT, HOJE, "op@astro.com")
        assert ok is False
        assert server._dia_etapa(UNIT, HOJE) == 1

    def test_etapa_3_nao_eh_afetada(self):
        server._iniciar_conferencia(UNIT, HOJE, "op@astro.com")
        server._fechar_dia(UNIT, HOJE, "op@astro.com")
        assert server._dia_etapa(UNIT, HOJE) == 3
        ok = server._voltar_para_lancamentos(UNIT, HOJE, "op@astro.com")
        assert ok is False  # nao volta — exige reabertura formal
        assert server._dia_etapa(UNIT, HOJE) == 3

    def test_volta_grava_historico(self):
        server._iniciar_conferencia(UNIT, HOJE, "op@astro.com")
        server._voltar_para_lancamentos(UNIT, HOJE, "outro@astro.com")
        fechs = server._load_fechamentos(UNIT)
        hist = fechs.get("_voltas_etapa", [])
        assert len(hist) == 1
        assert hist[0]["voltada_por"] == "outro@astro.com"
        assert hist[0]["iniciada_por"] == "op@astro.com"


# ══════════════════════════════════════════════════════════════════════════════
# Endpoint /api/fechamento/voltar-para-lancamentos
# ══════════════════════════════════════════════════════════════════════════════

class TestEndpoint:
    def test_em_etapa_2_volta_e_retorna_200(self, client):
        server._iniciar_conferencia(UNIT, HOJE, "op@astro.com")
        r = client.post(f"/u/{UNIT}/api/fechamento/voltar-para-lancamentos", json={})
        assert r.status_code == 200
        assert r.get_json()["success"] is True
        assert r.get_json()["etapa"] == 1

    def test_em_etapa_1_retorna_400(self, client):
        # Nada iniciado — nao tem o que voltar
        r = client.post(f"/u/{UNIT}/api/fechamento/voltar-para-lancamentos", json={})
        assert r.status_code == 400


# ══════════════════════════════════════════════════════════════════════════════
# Lancar em etapa 2 sem PIN
# ══════════════════════════════════════════════════════════════════════════════

class TestLancarEtapa2:
    def test_etapa_2_aceita_lancamento_sem_pin(self, client):
        # Inicia etapa 2
        server._iniciar_conferencia(UNIT, HOJE, "op@astro.com")
        # Lanca SEM pin → deve aceitar (etapa 2 nao bloqueia mais)
        r = client.post(f"/u/{UNIT}/api/caixa/lancar", json={
            "placa": "ABC1234", "cliente": "TESTE",
            "servico": "VISTORIA CAUTELAR", "valor": 100.0, "fp": "pix",
        })
        assert r.status_code == 200, r.get_data(as_text=True)

    def test_etapa_3_continua_exigindo_pin(self, client):
        # Etapa 3 (fechado) — exige PIN
        server._iniciar_conferencia(UNIT, HOJE, "op@astro.com")
        server._fechar_dia(UNIT, HOJE, "op@astro.com")
        r = client.post(f"/u/{UNIT}/api/caixa/lancar", json={
            "placa": "ABC1234", "cliente": "TESTE",
            "servico": "VISTORIA CAUTELAR", "valor": 100.0, "fp": "pix",
        })
        assert r.status_code == 403  # sem pin → bloqueado
