"""
Testes do estágio 7 — resiliência e segurança.

Cobre:
- Rate limiting no PIN: bloqueia após 10 tentativas, libera por unidade/IP
- Retry no Tiny: tenta 3x antes de falhar, não retenta duplicata
- Limite de payload: rejeita request acima de 1MB
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

os.environ.setdefault("SECRET_KEY", "test-secret-key-pytest")
os.environ.setdefault("USERS_CONFIG", "{}")
os.environ.setdefault("UNITS_CONFIG", "{}")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import server  # noqa: E402

UNIT = "testunit"
FAKE_USER = {"email": "op@astro.com", "name": "Op", "unit": UNIT, "master": False}


@pytest.fixture(autouse=True)
def reset_pin_attempts():
    """Limpa o estado de rate limiting entre testes."""
    server._pin_attempts.clear()
    yield
    server._pin_attempts.clear()


@pytest.fixture
def client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = {UNIT: {"nome": "Test", "master_pin": "1234"}}
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=FAKE_USER):
        with server.app.test_client() as c:
            yield c


def _lancar(client, **kwargs):
    payload = {"placa": "ABC1234", "cliente": "CLI", "servico": "LAUDO", "valor": 100.0, "fp": "pix", **kwargs}
    return client.post(f"/u/{UNIT}/api/caixa/lancar", json=payload)


def _excluir(client, lc_id, pin="1234"):
    return client.delete(f"/u/{UNIT}/api/caixa/excluir/{lc_id}", json={"pin": pin})


def _editar(client, lc_id, pin="1234"):
    return client.put(f"/u/{UNIT}/api/caixa/editar/{lc_id}",
                      json={"pin": pin, "placa": "ZZZ9999", "cliente": "C",
                            "servico": "LAUDO", "valor": 50.0, "fp": "pix"})


# ══════════════════════════════════════════════════════════════════════════════
# Rate limiting no PIN
# ══════════════════════════════════════════════════════════════════════════════

class TestPinRateLimit:
    def test_primeiras_tentativas_permitidas(self, client):
        lc_id = _lancar(client).get_json()["lancamento"]["id"]
        for _ in range(5):
            r = _excluir(client, lc_id, pin="0000")
            assert r.status_code == 403  # pin errado, mas não bloqueado

    def test_bloqueia_apos_limite(self, client):
        lc_id = _lancar(client).get_json()["lancamento"]["id"]
        for _ in range(server._PIN_MAX):
            _excluir(client, lc_id, pin="0000")
        r = _excluir(client, lc_id, pin="0000")
        assert r.status_code == 429

    def test_429_tem_mensagem_clara(self, client):
        lc_id = _lancar(client).get_json()["lancamento"]["id"]
        for _ in range(server._PIN_MAX):
            _excluir(client, lc_id, pin="0000")
        body = _excluir(client, lc_id, pin="0000").get_json()
        assert "Aguarde" in body["error"]

    def test_bloqueia_editar_tambem(self, client):
        lc_id = _lancar(client).get_json()["lancamento"]["id"]
        for _ in range(server._PIN_MAX):
            _editar(client, lc_id, pin="0000")
        r = _editar(client, lc_id, pin="0000")
        assert r.status_code == 429

    def test_pin_correto_apos_bloqueio_ainda_bloqueado(self, client):
        """Bloqueio é por IP, não por pin — pin correto não desbloqueia."""
        lc_id = _lancar(client).get_json()["lancamento"]["id"]
        for _ in range(server._PIN_MAX):
            _excluir(client, lc_id, pin="0000")
        r = _excluir(client, lc_id, pin="1234")
        assert r.status_code == 429


# ══════════════════════════════════════════════════════════════════════════════
# Retry com backoff no Tiny
# ══════════════════════════════════════════════════════════════════════════════

BASE_RECORD = {
    "id": "r1", "data": "2026-04-17", "modelo": "GOL",
    "placa": "ABC1234", "cliente": "CLI", "servico": "LAUDO",
    "fp": "AV", "preco": "100.00", "avPagamento": "AV",
    "origemArquivo": "planilha.xlsx", "linhaOrigem": 1,
}


class TestRetryTiny:
    def _send(self, client, records=None):
        return client.post(f"/u/{UNIT}/api/send",
                           json={"records": records or [BASE_RECORD]})

    def test_sucesso_na_primeira_tentativa(self, client):
        mock = MagicMock()
        mock.create_accounts_receivable.return_value = {"id": 1}
        with patch("server.TinyImporter", return_value=mock):
            with patch("server.time.sleep") as mock_sleep:
                body = self._send(client).get_json()
        assert len(body["summary"]["enviados"]) == 1
        mock_sleep.assert_not_called()

    def test_retenta_em_falha_transitoria(self, client):
        mock = MagicMock()
        mock.create_accounts_receivable.side_effect = [
            Exception("timeout"),
            Exception("timeout"),
            {"id": 99},
        ]
        with patch("server.TinyImporter", return_value=mock):
            with patch("server.time.sleep"):
                body = self._send(client).get_json()
        assert len(body["summary"]["enviados"]) == 1
        assert mock.create_accounts_receivable.call_count == 3

    def test_falha_apos_3_tentativas(self, client):
        mock = MagicMock()
        mock.create_accounts_receivable.side_effect = Exception("Tiny fora do ar")
        with patch("server.TinyImporter", return_value=mock):
            with patch("server.time.sleep"):
                body = self._send(client).get_json()
        assert len(body["summary"]["falhas"]) == 1
        assert mock.create_accounts_receivable.call_count == 3

    def test_nao_retenta_duplicata(self, client):
        mock = MagicMock()
        mock.create_accounts_receivable.side_effect = Exception("doc duplicado")
        with patch("server.TinyImporter", return_value=mock):
            with patch("server._is_doc_already_registered", return_value=True):
                with patch("server.time.sleep") as mock_sleep:
                    body = self._send(client).get_json()
        mock_sleep.assert_not_called()
        assert mock.create_accounts_receivable.call_count == 1

    def test_backoff_usa_sleep_correto(self, client):
        mock = MagicMock()
        mock.create_accounts_receivable.side_effect = [
            Exception("err1"),
            Exception("err2"),
            Exception("err3"),
        ]
        with patch("server.TinyImporter", return_value=mock):
            with patch("server.time.sleep") as mock_sleep:
                self._send(client)
        # Espera 2^0=1s e 2^1=2s entre tentativas
        assert mock_sleep.call_args_list == [call(1), call(2)]


# ══════════════════════════════════════════════════════════════════════════════
# Limite de payload
# ══════════════════════════════════════════════════════════════════════════════

class TestPayloadLimit:
    def test_payload_normal_aceito(self, client):
        r = _lancar(client)
        assert r.status_code == 200

    def test_payload_gigante_rejeitado(self, client):
        payload = {"placa": "ABC1234", "cliente": "C" * (2 * 1024 * 1024),
                   "servico": "LAUDO", "valor": 100.0, "fp": "pix"}
        r = client.post(f"/u/{UNIT}/api/caixa/lancar", json=payload)
        assert r.status_code == 413
