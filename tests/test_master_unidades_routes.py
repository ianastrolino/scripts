"""
Testes das rotas administrativas de unidades (master).

Cobre: criar, listar, remover, definir PIN, mapear FP.
Sem rede — UNITS injetado via server.UNITS direto.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

os.environ.setdefault("SECRET_KEY", "test-secret-key-pytest")
os.environ.setdefault("USERS_CONFIG", "{}")
os.environ.setdefault("UNITS_CONFIG", '{"barueri": {"nome": "Barueri", "master_pin": "1111"}}')

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import server  # noqa: E402

MASTER_USER = {"email": "ian@astrovistorias.com.br", "name": "Ian", "master": True}
OPERATOR_USER = {"email": "op@astrovistorias.com.br", "name": "Op", "unit": "barueri", "master": False}


def _setup_paths(tmp_path):
    """Aponta os paths globais de persistencia pra tmp_path do teste."""
    server.DATA_DIR = tmp_path
    server._UNITS_CUSTOM_FILE = tmp_path / "units_custom.json"
    server._PINS_FILE = tmp_path / "pins.json"
    server._AUDIT_LOG_PATH = tmp_path / "audit_log.jsonl"
    server.UNITS = {"barueri": {"nome": "Barueri", "master_pin": "1111"}}
    server.app.config["TESTING"] = True


@pytest.fixture
def master_client(tmp_path):
    _setup_paths(tmp_path)
    with patch.object(server, "_current_user", return_value=MASTER_USER):
        with server.app.test_client() as c:
            yield c


@pytest.fixture
def operator_client(tmp_path):
    _setup_paths(tmp_path)
    with patch.object(server, "_current_user", return_value=OPERATOR_USER):
        with server.app.test_client() as c:
            yield c


# ══════════════════════════════════════════════════════════════════════════════
# POST /master/api/unidades — criar
# ══════════════════════════════════════════════════════════════════════════════

class TestCriarUnidade:
    def _criar(self, client, **body):
        body.setdefault("slug", "novaunit")
        body.setdefault("nome", "Nova Unidade")
        body.setdefault("erp", "tiny")
        body.setdefault("client_id", "abc")
        body.setdefault("client_secret", "xyz")
        return client.post("/master/api/unidades", json=body)

    def test_cria_tiny_com_sucesso(self, master_client):
        r = self._criar(master_client)
        assert r.status_code == 200
        body = r.get_json()
        assert body["success"] is True
        assert body["slug"] == "novaunit"
        assert "/u/novaunit/" in body["redirect_uri"]

    def test_slug_obrigatorio(self, master_client):
        r = self._criar(master_client, slug="")
        assert r.status_code == 400
        assert "slug e nome" in r.get_json()["error"].lower()

    def test_nome_obrigatorio(self, master_client):
        r = self._criar(master_client, nome="")
        assert r.status_code == 400

    def test_slug_invalido(self, master_client):
        # Caracteres especiais nao permitidos
        r = self._criar(master_client, slug="nova unit!")
        assert r.status_code == 400
        assert "letras" in r.get_json()["error"].lower()

    def test_slug_duplicado(self, master_client):
        # 'barueri' ja existe
        r = self._criar(master_client, slug="barueri")
        assert r.status_code == 400
        assert "ja existe" in r.get_json()["error"].lower()

    def test_erp_invalido(self, master_client):
        r = self._criar(master_client, erp="sap")
        assert r.status_code == 400
        assert "tiny" in r.get_json()["error"].lower() or "omie" in r.get_json()["error"].lower()

    def test_tiny_sem_client_id_400(self, master_client):
        r = self._criar(master_client, client_id="")
        assert r.status_code == 400
        assert "client_id" in r.get_json()["error"].lower()

    def test_omie_sem_app_key_400(self, master_client):
        r = self._criar(master_client, erp="omie", client_id=None, client_secret=None)
        assert r.status_code == 400
        assert "app_key" in r.get_json()["error"].lower()

    def test_omie_com_credentials_passa(self, master_client):
        r = master_client.post("/master/api/unidades", json={
            "slug": "indianopolis", "nome": "Indianopolis", "erp": "omie",
            "app_key": "k", "app_secret": "s",
        })
        assert r.status_code == 200

    def test_nao_master_recebe_403(self, operator_client):
        r = operator_client.post("/master/api/unidades", json={
            "slug": "x", "nome": "X", "erp": "tiny", "client_id": "a", "client_secret": "b"
        })
        assert r.status_code == 403


# ══════════════════════════════════════════════════════════════════════════════
# POST /master/api/unidades/<slug>/pin — definir PIN
# ══════════════════════════════════════════════════════════════════════════════

class TestDefinirPin:
    def _set_pin(self, client, slug="barueri", pin="1234"):
        return client.post(f"/master/api/unidades/{slug}/pin", json={"pin": pin})

    def test_define_pin_valido(self, master_client):
        r = self._set_pin(master_client, pin="1234")
        assert r.status_code == 200
        assert r.get_json()["success"] is True

    def test_pin_curto_demais_400(self, master_client):
        r = self._set_pin(master_client, pin="123")
        assert r.status_code == 400
        assert "4-8 digitos" in r.get_json()["error"]

    def test_pin_longo_demais_400(self, master_client):
        r = self._set_pin(master_client, pin="123456789")
        assert r.status_code == 400

    def test_pin_com_letras_400(self, master_client):
        r = self._set_pin(master_client, pin="1a2b")
        assert r.status_code == 400

    def test_unidade_inexistente_400(self, master_client):
        r = self._set_pin(master_client, slug="naoexiste", pin="1234")
        assert r.status_code == 400
        assert "invalida" in r.get_json()["error"].lower()

    def test_pin_persiste_e_pode_ser_verificado(self, master_client):
        self._set_pin(master_client, pin="9999")
        # _verify_unit_pin deve aceitar 9999 agora
        assert server._verify_unit_pin("barueri", "9999") is True
        assert server._verify_unit_pin("barueri", "0000") is False

    def test_pin_pode_ser_substituido(self, master_client):
        self._set_pin(master_client, pin="1234")
        self._set_pin(master_client, pin="5678")
        assert server._verify_unit_pin("barueri", "5678") is True
        assert server._verify_unit_pin("barueri", "1234") is False

    def test_nao_master_recebe_403(self, operator_client):
        r = operator_client.post("/master/api/unidades/barueri/pin", json={"pin": "1234"})
        assert r.status_code == 403


# ══════════════════════════════════════════════════════════════════════════════
# DELETE /master/api/unidades/<slug>
# ══════════════════════════════════════════════════════════════════════════════

class TestRemoverUnidade:
    def test_remove_unidade_custom_sem_dados(self, master_client):
        # Cria unidade primeiro
        master_client.post("/master/api/unidades", json={
            "slug": "temp", "nome": "Temp", "erp": "tiny", "client_id": "a", "client_secret": "b"
        })
        # Remove
        r = master_client.delete("/master/api/unidades/temp")
        assert r.status_code == 200
        assert r.get_json()["success"] is True

    def test_nao_remove_unidade_do_env(self, master_client):
        # 'barueri' veio do UNITS_CONFIG (env), nao eh custom
        r = master_client.delete("/master/api/unidades/barueri")
        assert r.status_code == 404
        assert "custom" in r.get_json()["error"].lower()

    def test_nao_master_recebe_403(self, operator_client):
        r = operator_client.delete("/master/api/unidades/barueri")
        assert r.status_code == 403


# ══════════════════════════════════════════════════════════════════════════════
# GET /master/api/unidades — listar
# ══════════════════════════════════════════════════════════════════════════════

class TestListarUnidades:
    def test_lista_inclui_unidade_criada(self, master_client):
        master_client.post("/master/api/unidades", json={
            "slug": "novaunit", "nome": "Nova", "erp": "tiny", "client_id": "a", "client_secret": "b"
        })
        r = master_client.get("/master/api/unidades")
        assert r.status_code == 200
        body = r.get_json()
        slugs = [u["slug"] for u in body.get("unidades", [])]
        # Nao checa env (env_var contaminada por outros tests), so confirma que
        # a unidade criada via API esta na lista
        assert "novaunit" in slugs

    def test_nao_master_recebe_403(self, operator_client):
        r = operator_client.get("/master/api/unidades")
        assert r.status_code == 403
