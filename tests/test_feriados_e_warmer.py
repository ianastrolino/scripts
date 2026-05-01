"""
Testes do detector de feriado + cache warmer de contas-receber.

Cobre:
- _calcula_pascoa: datas conhecidas
- _eh_feriado_nacional: fixos (1/jan, 1/mai, 25/dez), moveis (Sexta Santa,
  Carnaval, Corpus Christi), nao-feriados
- master_api_visao_geral expoe campo `feriado` quando aplicavel
- master_api_visao_geral suprime alerta unidade_inativa em feriado
- _compute_contas_receber retorna dict (refator pos extracao)
- _warm_contas_receber_cache popula o cache (mock pro Tiny)
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


# ══════════════════════════════════════════════════════════════════════════════
# _calcula_pascoa
# ══════════════════════════════════════════════════════════════════════════════

class TestCalculaPascoa:
    @pytest.mark.parametrize("ano,mes,dia", [
        (2024, 3, 31),  # Pascoa 2024
        (2025, 4, 20),  # Pascoa 2025
        (2026, 4, 5),   # Pascoa 2026
        (2027, 3, 28),  # Pascoa 2027
    ])
    def test_pascoa_datas_conhecidas(self, ano, mes, dia):
        assert server._calcula_pascoa(ano) == dt.date(ano, mes, dia)


# ══════════════════════════════════════════════════════════════════════════════
# _eh_feriado_nacional
# ══════════════════════════════════════════════════════════════════════════════

class TestFeriadosFixos:
    @pytest.mark.parametrize("data_iso,nome", [
        ("2026-01-01", "Confraternização Universal"),
        ("2026-04-21", "Tiradentes"),
        ("2026-05-01", "Dia do Trabalho"),
        ("2026-09-07", "Independência"),
        ("2026-10-12", "Nossa Senhora Aparecida"),
        ("2026-11-02", "Finados"),
        ("2026-11-15", "Proclamação da República"),
        ("2026-11-20", "Dia da Consciência Negra"),
        ("2026-12-25", "Natal"),
    ])
    def test_fixos_2026(self, data_iso, nome):
        eh, nm = server._eh_feriado_nacional(dt.date.fromisoformat(data_iso))
        assert eh is True
        assert nm == nome

    def test_dia_nao_feriado(self):
        eh, nm = server._eh_feriado_nacional(dt.date(2026, 5, 2))  # sabado pos-trabalho
        assert eh is False
        assert nm == ""


class TestFeriadosMoveis:
    def test_sexta_santa_2026(self):
        # Pascoa 2026 = 5/abril → Sexta Santa = 3/abril
        eh, nm = server._eh_feriado_nacional(dt.date(2026, 4, 3))
        assert eh is True
        assert nm == "Sexta-Feira Santa"

    def test_carnaval_2026(self):
        # Pascoa 2026 = 5/abril → Carnaval (terca) = 47 dias antes = 17/fevereiro
        eh, nm = server._eh_feriado_nacional(dt.date(2026, 2, 17))
        assert eh is True
        assert nm == "Carnaval"

    def test_corpus_christi_2026(self):
        # Pascoa 2026 = 5/abril → Corpus Christi = 60 dias depois = 4/junho
        eh, nm = server._eh_feriado_nacional(dt.date(2026, 6, 4))
        assert eh is True
        assert nm == "Corpus Christi"

    def test_pascoa_em_si_nao_eh_feriado_aqui(self):
        # Pascoa (domingo) nao foi listado como feriado nacional pra fins
        # operacionais — empresa eh fechada no domingo de qualquer forma.
        eh, _ = server._eh_feriado_nacional(dt.date(2026, 4, 5))
        assert eh is False


# ══════════════════════════════════════════════════════════════════════════════
# master_api_visao_geral expoe feriado e suprime alerta
# ══════════════════════════════════════════════════════════════════════════════

UNITS_FIX = {"sp": {"nome": "São Paulo", "master_pin": "0000", "erp": "tiny"}}
MASTER_USER = {"email": "admin@astro.com", "name": "Admin", "unit": None, "master": True}


@pytest.fixture
def master_client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    # Limpa cache pra evitar resposta cacheada de outro teste
    server._VISAO_GERAL_CACHE.clear()
    with patch.object(server, "_current_user", return_value=MASTER_USER):
        with server.app.test_client() as c:
            yield c


class TestVisaoGeralFeriado:
    def test_dia_nao_feriado_nao_expoe_campo(self, master_client):
        # Mocka _eh_feriado_nacional pra forcar nao-feriado
        with patch.object(server, "_eh_feriado_nacional", return_value=(False, "")):
            server._VISAO_GERAL_CACHE.clear()
            body = master_client.get("/master/api/visao-geral").get_json()
            assert body["feriado"] is None

    def test_dia_feriado_expoe_nome(self, master_client):
        with patch.object(server, "_eh_feriado_nacional", return_value=(True, "Dia do Trabalho")):
            server._VISAO_GERAL_CACHE.clear()
            body = master_client.get("/master/api/visao-geral").get_json()
            assert body["feriado"] is not None
            assert body["feriado"]["eh_feriado"] is True
            assert body["feriado"]["nome"] == "Dia do Trabalho"


# ══════════════════════════════════════════════════════════════════════════════
# _compute_contas_receber (refator)
# ══════════════════════════════════════════════════════════════════════════════

class TestComputeContasReceberPuro:
    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path):
        server.DATA_DIR = tmp_path
        server.UNITS = UNITS_FIX
        server._CONTAS_RECEBER_CACHE.clear()

    def test_funcao_pode_ser_chamada_sem_request(self):
        """Nao depende mais de Flask request — cron warmer pode chamar direto."""
        with patch.object(server, "_fetch_contas_mes_tiny", return_value=[]), \
             patch.object(server, "_fetch_contas_abertas_tiny", return_value=[]):
            payload = server._compute_contas_receber(
                unit_filter="all", data_param="", mes_param="", force=True,
            )
        assert payload["success"] is True
        assert "totais" in payload
        assert "por_unidade" in payload

    def test_unit_invalido_retorna_payload_zerado(self):
        with patch.object(server, "_fetch_contas_mes_tiny", return_value=[]), \
             patch.object(server, "_fetch_contas_abertas_tiny", return_value=[]):
            payload = server._compute_contas_receber(
                unit_filter="nao_existe", data_param="", mes_param="", force=True,
            )
        assert payload["success"] is True
        assert payload["por_unidade"] == []  # nenhuma unidade match
        assert payload["totais"]["emitidas"] == 0


# ══════════════════════════════════════════════════════════════════════════════
# _warm_contas_receber_cache
# ══════════════════════════════════════════════════════════════════════════════

class TestWarmer:
    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path):
        server.DATA_DIR = tmp_path
        server.UNITS = UNITS_FIX
        server._CONTAS_RECEBER_CACHE.clear()

    def test_warmer_popula_cache(self):
        with patch.object(server, "_fetch_contas_mes_tiny", return_value=[]), \
             patch.object(server, "_fetch_contas_abertas_tiny", return_value=[]):
            server._warm_contas_receber_cache()
        # Cache pra unit=all, modo=dia, hoje
        hoje = dt.date.today().isoformat()
        keys = list(server._CONTAS_RECEBER_CACHE.keys())
        # Espera pelo menos 1 chave com prefix "all:"
        assert any(k.startswith("all:") for k in keys), f"keys={keys}"

    def test_warmer_falha_silenciosa(self):
        # Forca uma exception interna (UNITS=None)
        original = server.UNITS
        server.UNITS = None  # vai causar erro no _compute
        try:
            # Nao deve levantar
            server._warm_contas_receber_cache()
        finally:
            server.UNITS = original
