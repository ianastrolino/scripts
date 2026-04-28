"""
Testes do estágio 6 — observabilidade.

Cobre:
- GET /health: disponibilidade, campos, sem autenticação
- @master_required: 401 sem login, 403 para operador, 200 para master
- app.logger.exception: erros de rota chegam ao log (não são engolidos)
"""
from __future__ import annotations

import logging
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

MASTER_USER   = {"email": "admin@astro.com", "name": "Admin", "unit": None,  "master": True}
OPERATOR_USER = {"email": "op@astro.com",    "name": "Op",    "unit": "sp",  "master": False}


@pytest.fixture
def anon_client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = {"sp": {"nome": "São Paulo", "master_pin": "0000"}}
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=None):
        with server.app.test_client() as c:
            yield c


@pytest.fixture
def master_client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = {"sp": {"nome": "São Paulo", "master_pin": "0000"}}
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=MASTER_USER):
        with server.app.test_client() as c:
            yield c


@pytest.fixture
def operator_client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = {"sp": {"nome": "São Paulo", "master_pin": "0000"}}
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=OPERATOR_USER):
        with server.app.test_client() as c:
            yield c


# ══════════════════════════════════════════════════════════════════════════════
# /health
# ══════════════════════════════════════════════════════════════════════════════

class TestHealth:
    def test_retorna_200_sem_autenticacao(self, anon_client):
        r = anon_client.get("/health")
        assert r.status_code == 200

    def test_status_ok_ou_degraded(self, anon_client):
        # Aceita "ok" (sem warnings) ou "degraded" (warnings nao-criticos como
        # tokens Tiny faltando). Ambos retornam HTTP 200. "unhealthy" = 503.
        body = anon_client.get("/health").get_json()
        assert body["status"] in ("ok", "degraded")

    def test_retorna_contagem_de_unidades(self, anon_client):
        body = anon_client.get("/health").get_json()
        assert body["units"] == 1

    def test_retorna_timestamp(self, anon_client):
        body = anon_client.get("/health").get_json()
        assert "ts" in body
        assert "T" in body["ts"]  # formato ISO

    def test_inclui_checks_estruturados(self, anon_client):
        body = anon_client.get("/health").get_json()
        for k in ("app", "db", "disk", "fs_write", "tokens"):
            assert k in body["checks"], f"check {k} faltando"
            assert "ok" in body["checks"][k]

    def test_check_db_mede_latencia(self, anon_client):
        body = anon_client.get("/health").get_json()
        db = body["checks"]["db"]
        assert db["ok"] is True
        # latency_ms presente quando db check passou
        assert "latency_ms" in db
        assert db["latency_ms"] >= 0

    def test_check_disk_inclui_metrics(self, anon_client):
        body = anon_client.get("/health").get_json()
        disk = body["checks"]["disk"]
        assert "free_pct" in disk
        assert "free_mb" in disk
        assert "total_mb" in disk

    def test_uptime_em_segundos(self, anon_client):
        body = anon_client.get("/health").get_json()
        assert "uptime_s" in body
        assert isinstance(body["uptime_s"], (int, float))
        assert body["uptime_s"] >= 0

    def test_cache_funciona(self, anon_client):
        # 1a chamada — calcula
        body1 = anon_client.get("/health").get_json()
        assert body1.get("cached") in (False, None)
        # 2a chamada imediata — usa cache
        body2 = anon_client.get("/health").get_json()
        assert body2.get("cached") is True

    def test_db_inacessivel_retorna_503(self, anon_client, monkeypatch):
        from unittest.mock import patch
        # Forca _health_check_db a retornar erro
        with patch.object(server, "_health_check_db", return_value={"ok": False, "error": "simulated"}):
            r = anon_client.get("/health")
            body = r.get_json()
            assert r.status_code == 503
            assert body["status"] == "unhealthy"
            assert body["checks"]["db"]["ok"] is False

    def test_disco_lotado_retorna_503(self, anon_client):
        from unittest.mock import patch
        with patch.object(server, "_health_check_disk", return_value={"ok": False, "free_pct": 1.0, "warning": True}):
            r = anon_client.get("/health")
            assert r.status_code == 503

    def test_warning_em_tokens_nao_derruba_health(self, anon_client):
        from unittest.mock import patch
        # tokens warning (warning=True, ok=True) → degraded, mas 200
        with patch.object(server, "_health_check_tokens", return_value={"ok": True, "warning": True, "ativos": 0, "total_tiny": 1}):
            r = anon_client.get("/health")
            body = r.get_json()
            assert r.status_code == 200
            assert body["status"] in ("ok", "degraded")


# ══════════════════════════════════════════════════════════════════════════════
# @master_required
# ══════════════════════════════════════════════════════════════════════════════

class TestMasterRequired:
    def test_sem_login_retorna_401(self, anon_client):
        r = anon_client.get("/master/api/units")
        assert r.status_code == 401

    def test_operador_retorna_403(self, operator_client):
        r = operator_client.get("/master/api/units")
        assert r.status_code == 403

    def test_master_retorna_200(self, master_client):
        r = master_client.get("/master/api/units")
        assert r.status_code == 200

    def test_status_sem_login_retorna_401(self, anon_client):
        r = anon_client.get("/master/api/units/status")
        assert r.status_code == 401

    def test_status_operador_retorna_403(self, operator_client):
        r = operator_client.get("/master/api/units/status")
        assert r.status_code == 403


# ══════════════════════════════════════════════════════════════════════════════
# logging de erros de rota
# ══════════════════════════════════════════════════════════════════════════════

class TestLoggingDeErros:
    def _setup(self, tmp_path):
        server.DATA_DIR = tmp_path
        server.UNITS = {"sp": {"nome": "SP", "master_pin": "0000"}}
        server.app.config["TESTING"] = True

    def test_excecao_retorna_json_com_erro(self, tmp_path):
        """Mesmo com exceção, a rota retorna JSON estruturado — não uma página de erro HTML."""
        self._setup(tmp_path)
        fake_user = {"email": "op@astro.com", "name": "Op", "unit": "sp", "master": False}
        with patch.object(server, "_current_user", return_value=fake_user):
            with patch.object(server, "_load_caixa_dia", side_effect=RuntimeError("db explodiu")):
                with server.app.test_client() as c:
                    r = c.get("/u/sp/api/caixa/estado")
        assert r.status_code == 500
        body = r.get_json()
        assert body["success"] is False
        assert "db explodiu" in body["error"]

    def test_logger_exception_chamado_em_erro_de_rota(self, tmp_path):
        """Verifica que app.logger.exception é chamado quando uma rota lança exceção."""
        self._setup(tmp_path)
        fake_user = {"email": "op@astro.com", "name": "Op", "unit": "sp", "master": False}
        with patch.object(server, "_current_user", return_value=fake_user):
            with patch.object(server, "_load_caixa_dia", side_effect=RuntimeError("falha proposital")):
                with patch.object(server.app.logger, "exception") as mock_log:
                    with server.app.test_client() as c:
                        c.get("/u/sp/api/caixa/estado")
        mock_log.assert_called_once()
        assert "/u/sp/api/caixa/estado" in mock_log.call_args[0][1]

    def test_path_da_rota_aparece_no_log(self, tmp_path):
        """O path HTTP aparece no log para facilitar triagem no Railway."""
        self._setup(tmp_path)
        fake_user = {"email": "op@astro.com", "name": "Op", "unit": "sp", "master": False}
        with patch.object(server, "_current_user", return_value=fake_user):
            with patch.object(server, "_load_caixa_dia", side_effect=RuntimeError("x")):
                with patch.object(server.app.logger, "exception") as mock_log:
                    with server.app.test_client() as c:
                        c.get("/u/sp/api/caixa/estado")
        log_args = mock_log.call_args[0]
        assert "sp" in log_args[1] or "sp" in str(log_args)
