"""
Fixtures globais para toda a suite de testes.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import server


@pytest.fixture(autouse=True)
def reset_rate_limits():
    """Limpa contadores de rate limiting entre testes para evitar contaminação."""
    server._pin_attempts.clear()
    server._login_attempts.clear()
    yield
    server._pin_attempts.clear()
    server._login_attempts.clear()


@pytest.fixture(autouse=True)
def reset_health_cache():
    """Limpa cache do healthcheck pra evitar contaminacao entre testes."""
    server._HEALTH_CACHE.update({"ts": 0.0, "payload": None, "code": 200})
    yield
    server._HEALTH_CACHE.update({"ts": 0.0, "payload": None, "code": 200})


@pytest.fixture(autouse=True)
def reset_planilha_status_cache():
    """Limpa cache do /api/planilha/status entre testes (era global, contaminava
    asserts cross-test que mudavam mtime/data muito rapido)."""
    server._PLANILHA_STATUS_CACHE.clear()
    yield
    server._PLANILHA_STATUS_CACHE.clear()
