"""
Testes da deduplicacao de envios_tiny e do contador de vistorias.

Bug confirmado em Moema 04/05/2026:
- Painel mostrou 84 lancamentos, real era 44 (envio duplicado pelo Tiny)
- Causa: record_key incluia modelo. Reimport da planilha apos fix do parser
  Sispevi capturou modelo, gerando record_key divergente do envio anterior.
  Dedup do banco nao pegou (UNIQUE constraint no chave_deduplicacao).

Testes:
- record_key NAO inclui modelo (mudanca de modelo nao gera duplicata)
- record_key inclui campos de identidade (data/placa/cliente/servico/preco)
- _resumo_dia_unit deduplica por (placa, servico, valor, fp)
- master_api_units_status idem
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
from caixa_db import insert_envio_tiny  # noqa: E402
from tiny_import import record_key  # noqa: E402


# ══════════════════════════════════════════════════════════════════════════════
# record_key
# ══════════════════════════════════════════════════════════════════════════════

class TestRecordKey:
    def _base_record(self, **overrides):
        base = {
            "data":    "2026-05-04",
            "modelo":  "FORD KA",
            "placa":   "ABC1234",
            "cliente": "TESTE LTDA",
            "servico": "VISTORIA CAUTELAR",
            "preco":   "100.00",
        }
        base.update(overrides)
        return base

    def test_key_estavel_pra_mesmo_input(self):
        r1 = self._base_record()
        r2 = self._base_record()
        assert record_key(r1) == record_key(r2)

    def test_modelo_diferente_NAO_muda_a_key(self):
        """Bug fix: modelo era incluido na key. Reimport com parser melhorado
        capturava modelo, gerando key diferente, e duplicava envio."""
        r1 = self._base_record(modelo="")
        r2 = self._base_record(modelo="FORD KA")
        r3 = self._base_record(modelo="VW GOL")
        assert record_key(r1) == record_key(r2) == record_key(r3)

    def test_placa_diferente_muda_key(self):
        r1 = self._base_record(placa="ABC1234")
        r2 = self._base_record(placa="XYZ9999")
        assert record_key(r1) != record_key(r2)

    def test_servico_diferente_muda_key(self):
        r1 = self._base_record(servico="VISTORIA CAUTELAR")
        r2 = self._base_record(servico="LAUDO DE TRANSFERENCIA")
        assert record_key(r1) != record_key(r2)

    def test_preco_diferente_muda_key(self):
        r1 = self._base_record(preco="100.00")
        r2 = self._base_record(preco="200.00")
        assert record_key(r1) != record_key(r2)

    def test_cliente_diferente_muda_key(self):
        r1 = self._base_record(cliente="A")
        r2 = self._base_record(cliente="B")
        assert record_key(r1) != record_key(r2)


# ══════════════════════════════════════════════════════════════════════════════
# _resumo_dia_unit dedup
# ══════════════════════════════════════════════════════════════════════════════

UNITS_FIX = {"sp": {"nome": "São Paulo"}}
HOJE = dt.date.today().isoformat()


@pytest.fixture(autouse=True)
def _setup(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    yield


def _envio(unit, suffix, **overrides):
    """Helper: gera payload de envio_tiny com chave_deduplicacao unica
    (pra simular bug onde record_key divergiu entre envios)."""
    payload = {
        "chave_deduplicacao": f"key-{suffix}",
        "timestamp":          f"{HOJE}T10:00:00",
        "data_lancamento":    HOJE,
        "placa":              "ABC1234",
        "cliente":            "TESTE",
        "servico":            "CAUTELAR",
        "valor":              100.0,
        "fp":                 "FA",
        "status":             "ok",
    }
    payload.update(overrides)
    return payload


class TestResumoDiaUnitDedup:
    def test_envio_unico_conta_1(self):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        insert_envio_tiny("sp", unit_dir, _envio("sp", "1"))
        resumo = server._resumo_dia_unit("sp", unit_dir, HOJE)
        assert resumo["vistorias"] == 1

    def test_mesma_vistoria_chave_dedup_diferente_conta_1(self):
        """Bug fix: 2 envios com chave_deduplicacao distinta mas mesma
        (placa, servico, valor, fp) — sistema costumava contar 2."""
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        # Mesma vistoria, mas chave_deduplicacao diferente (record_key divergiu)
        insert_envio_tiny("sp", unit_dir, _envio("sp", "1a"))
        insert_envio_tiny("sp", unit_dir, _envio("sp", "1b"))
        resumo = server._resumo_dia_unit("sp", unit_dir, HOJE)
        assert resumo["vistorias"] == 1, "deveria deduplicar por (placa, servico, valor, fp)"

    def test_total_nao_duplica_quando_envio_duplica(self):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        insert_envio_tiny("sp", unit_dir, _envio("sp", "1a", valor=100.0))
        insert_envio_tiny("sp", unit_dir, _envio("sp", "1b", valor=100.0))
        resumo = server._resumo_dia_unit("sp", unit_dir, HOJE)
        # Total deve ser 100, nao 200 (mesma vistoria contada 1x)
        assert resumo["total"] == 100.0
        assert resumo["fa"] == 100.0

    def test_vistorias_diferentes_contam_separado(self):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        insert_envio_tiny("sp", unit_dir, _envio("sp", "1", placa="AAA1111", valor=100.0))
        insert_envio_tiny("sp", unit_dir, _envio("sp", "2", placa="BBB2222", valor=200.0))
        resumo = server._resumo_dia_unit("sp", unit_dir, HOJE)
        assert resumo["vistorias"] == 2
        assert resumo["total"] == 300.0
