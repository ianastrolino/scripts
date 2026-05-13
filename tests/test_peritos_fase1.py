"""
Testes da Fase 1 do relatorio por perito.

Captura + persistencia do campo PERITO da planilha Sispevi/Megalaudo:
- NormalizedRecord ganha campo perito (default "")
- envios_erp ganha coluna perito (migration retroativa)
- insert_envio_tiny grava perito
- /api/send aceita perito no payload e propaga pro banco
"""
from __future__ import annotations

import os
import sys
import datetime as dt
from pathlib import Path
from unittest.mock import patch

import pytest

os.environ.setdefault("SECRET_KEY", "test-secret-key-pytest")
os.environ.setdefault("USERS_CONFIG", "{}")
os.environ.setdefault("UNITS_CONFIG", "{}")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import server  # noqa: E402
from caixa_db import insert_envio_tiny, _connect  # noqa: E402
from tiny_import import NormalizedRecord  # noqa: E402


UNITS_FIX = {"sp": {"nome": "São Paulo"}}
USER = {"email": "op@astro.com", "name": "Op", "unit": "sp", "master": False}
HOJE = dt.date.today().isoformat()


@pytest.fixture(autouse=True)
def _setup(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    yield


# ══════════════════════════════════════════════════════════════════════════════
# NormalizedRecord aceita perito
# ══════════════════════════════════════════════════════════════════════════════

class TestNormalizedRecordPerito:
    def test_perito_default_vazio(self):
        rec = NormalizedRecord(
            data="2026-05-13", modelo="GOL", placa="ABC1234", cliente="X",
            servico="VISTORIA CAUTELAR", fp="AV", preco="100",
            origem_arquivo="t", linha_origem=1, chave_deduplicacao="x",
        )
        assert rec.perito == ""

    def test_perito_preenchido(self):
        rec = NormalizedRecord(
            data="2026-05-13", modelo="GOL", placa="ABC1234", cliente="X",
            servico="VISTORIA CAUTELAR", fp="AV", preco="100",
            origem_arquivo="t", linha_origem=1, chave_deduplicacao="x",
            perito="JOAO SILVA",
        )
        assert rec.perito == "JOAO SILVA"


# ══════════════════════════════════════════════════════════════════════════════
# envios_erp persiste perito
# ══════════════════════════════════════════════════════════════════════════════

class TestEnviosErpPerito:
    def _envio(self, **overrides):
        base = {
            "chave_deduplicacao": "key-1",
            "timestamp":          f"{HOJE}T10:00:00",
            "data_lancamento":    HOJE,
            "placa":              "ABC1234",
            "cliente":            "TESTE",
            "servico":            "CAUTELAR",
            "valor":              100.0,
            "fp":                 "FA",
            "status":             "enviado",
        }
        base.update(overrides)
        return base

    def test_insert_com_perito(self):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        insert_envio_tiny("sp", unit_dir, self._envio(perito="JOAO SILVA"))
        with _connect(unit_dir) as conn:
            row = conn.execute(
                "SELECT perito FROM envios_erp WHERE unit=? AND chave_deduplicacao=?",
                ("sp", "key-1"),
            ).fetchone()
        assert row["perito"] == "JOAO SILVA"

    def test_insert_sem_perito_vira_string_vazia(self):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        insert_envio_tiny("sp", unit_dir, self._envio())  # sem perito
        with _connect(unit_dir) as conn:
            row = conn.execute(
                "SELECT perito FROM envios_erp WHERE chave_deduplicacao='key-1'"
            ).fetchone()
        assert row["perito"] == ""

    def test_upsert_preserva_perito_anterior(self):
        """Reenvio do mesmo record com perito vazio NAO apaga perito ja salvo."""
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        # Primeiro envio com perito
        insert_envio_tiny("sp", unit_dir, self._envio(perito="JOAO SILVA"))
        # Reenvio sem perito (planilha antiga ou bug)
        insert_envio_tiny("sp", unit_dir, self._envio(status="enviado", perito=""))
        with _connect(unit_dir) as conn:
            row = conn.execute(
                "SELECT perito FROM envios_erp WHERE chave_deduplicacao='key-1'"
            ).fetchone()
        # Guard do upsert: vazio nao sobrescreve preenchido
        assert row["perito"] == "JOAO SILVA"

    def test_upsert_atualiza_perito_quando_vazio_antes(self):
        """Primeiro envio sem perito, segundo com — deve atualizar."""
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        insert_envio_tiny("sp", unit_dir, self._envio(perito=""))
        insert_envio_tiny("sp", unit_dir, self._envio(perito="MARIA"))
        with _connect(unit_dir) as conn:
            row = conn.execute(
                "SELECT perito FROM envios_erp WHERE chave_deduplicacao='key-1'"
            ).fetchone()
        assert row["perito"] == "MARIA"

    def test_index_unit_perito_existe(self):
        """Indice idx_envios_unit_perito necessario pro relatorio agrupar rapido."""
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        # Forca criacao do schema
        insert_envio_tiny("sp", unit_dir, self._envio(perito="X"))
        with _connect(unit_dir) as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='envios_erp'"
            ).fetchall()
            names = {r["name"] for r in rows}
        assert "idx_envios_unit_perito" in names


# ══════════════════════════════════════════════════════════════════════════════
# Migration retroativa: banco sem coluna perito ganha via ALTER
# ══════════════════════════════════════════════════════════════════════════════

class TestMigrationPerito:
    def test_banco_legacy_sem_perito_ganha_coluna(self, tmp_path):
        """Simula banco pre-Fase 1: cria envios_erp sem coluna perito,
        depois conecta — _ensure_column adiciona via ALTER."""
        import sqlite3
        unit_dir = tmp_path / "legacy"
        unit_dir.mkdir()
        db_path = unit_dir / "caixa_dia.db"
        conn = sqlite3.connect(str(db_path))
        # Tabela "antiga" (DDL sem perito)
        conn.execute("""
            CREATE TABLE envios_erp (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                unit               TEXT NOT NULL,
                erp                TEXT NOT NULL DEFAULT 'tiny',
                chave_deduplicacao TEXT NOT NULL,
                timestamp          TEXT NOT NULL,
                data_lancamento    TEXT NOT NULL DEFAULT "",
                placa              TEXT NOT NULL DEFAULT "",
                cliente            TEXT NOT NULL DEFAULT "",
                servico            TEXT NOT NULL DEFAULT "",
                valor              REAL NOT NULL DEFAULT 0,
                fp                 TEXT NOT NULL DEFAULT "",
                status             TEXT NOT NULL,
                arquivo            TEXT NOT NULL DEFAULT "",
                linha              INTEGER NOT NULL DEFAULT 0,
                resposta_tiny      TEXT NOT NULL DEFAULT "",
                erro               TEXT NOT NULL DEFAULT "",
                UNIQUE(unit, chave_deduplicacao)
            )
        """)
        conn.commit()
        conn.close()

        # Conecta via _connect — migration retroativa adiciona perito
        with _connect(unit_dir) as conn:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(envios_erp)").fetchall()}
        assert "perito" in cols
