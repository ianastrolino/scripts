"""
Testes do hardening de seguranca:
- Rate-limit no /api/log/js-error
- Headers de seguranca em toda resposta (CSP, X-Frame, X-Content-Type, HSTS)
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


@pytest.fixture
def client(tmp_path):
    server.DATA_DIR = tmp_path
    server._JS_ERRORS_PATH = tmp_path / "js_errors.jsonl"
    server.app.config["TESTING"] = True
    # Reset rate-limit state entre testes
    server._js_error_attempts.clear()
    with server.app.test_client() as c:
        yield c


# ══════════════════════════════════════════════════════════════════════════════
# Rate-limit /api/log/js-error
# ══════════════════════════════════════════════════════════════════════════════

class TestJsErrorRateLimit:
    def test_primeiras_requests_aceitas(self, client):
        for _ in range(5):
            r = client.post("/api/log/js-error", json={"message": "erro"})
            assert r.status_code == 200

    def test_passa_do_limite_retorna_429(self, client):
        # _JS_ERROR_MAX = 20 — passa do limite na 21a tentativa
        for _ in range(20):
            client.post("/api/log/js-error", json={"message": "erro"})
        r = client.post("/api/log/js-error", json={"message": "erro 21"})
        assert r.status_code == 429
        body = r.get_json()
        assert body.get("rate_limited") is True

    def test_helper_rate_check(self):
        server._js_error_attempts.clear()
        ip = "192.0.2.1"
        # Aceita ate o limite
        for i in range(server._JS_ERROR_MAX):
            assert server._js_error_rate_check(ip), f"falhou na tentativa {i}"
        # Bloqueia depois
        assert not server._js_error_rate_check(ip)

    def test_ips_diferentes_nao_se_atrapalham(self):
        server._js_error_attempts.clear()
        # IP A esgota cota
        for _ in range(server._JS_ERROR_MAX):
            server._js_error_rate_check("10.0.0.1")
        # IP B ainda funciona
        assert server._js_error_rate_check("10.0.0.2")


# ══════════════════════════════════════════════════════════════════════════════
# Headers de seguranca
# ══════════════════════════════════════════════════════════════════════════════

class TestSecurityHeaders:
    def test_x_content_type_options_nosniff(self, client):
        r = client.get("/health")
        assert r.headers.get("X-Content-Type-Options") == "nosniff"

    def test_x_frame_options_sameorigin(self, client):
        r = client.get("/health")
        assert r.headers.get("X-Frame-Options") == "SAMEORIGIN"

    def test_referrer_policy(self, client):
        r = client.get("/health")
        assert r.headers.get("Referrer-Policy") == "strict-origin-when-cross-origin"

    def test_csp_presente(self, client):
        r = client.get("/health")
        csp = r.headers.get("Content-Security-Policy", "")
        assert "default-src 'self'" in csp
        assert "frame-ancestors 'self'" in csp
        assert "base-uri 'self'" in csp

    def test_csp_permite_google_fonts(self, client):
        r = client.get("/health")
        csp = r.headers.get("Content-Security-Policy", "")
        assert "https://fonts.googleapis.com" in csp
        assert "https://fonts.gstatic.com" in csp

    def test_hsts_so_em_https(self, client):
        # Test client roda em HTTP — HSTS nao deve aparecer
        r = client.get("/health")
        assert "Strict-Transport-Security" not in r.headers

    def test_hsts_aparece_com_x_forwarded_proto_https(self, client):
        # Railway usa proxy reverso que adiciona X-Forwarded-Proto: https
        r = client.get("/health", headers={"X-Forwarded-Proto": "https"})
        hsts = r.headers.get("Strict-Transport-Security")
        assert hsts is not None
        assert "max-age=31536000" in hsts
        assert "includeSubDomains" in hsts

    def test_headers_aparecem_em_endpoint_autenticado(self, client):
        # Mesmo em rotas com auth, headers vao
        r = client.get("/api/me")
        # /api/me sem login redireciona, mas os headers vao na resposta
        assert r.headers.get("X-Frame-Options") == "SAMEORIGIN"
        assert r.headers.get("Content-Security-Policy") is not None
