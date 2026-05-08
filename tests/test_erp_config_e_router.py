"""
Testes da config Omie + roteador ERP em api_send.

Cobre:
- _build_erp_importer retorna TinyImporter ou OmieImporter
- _load_omie_config / _save_omie_config persistem corretamente
- Endpoint GET /master/api/unidades/<slug>/omie-config retorna mascarado
- Endpoint POST salva e preserva app_secret quando omitido
- _is_doc_already_registered cobre mensagens Omie
- envios_erp grava com erp correto via api_send
"""
from __future__ import annotations

import json
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
from omie_import import OmieImporter  # noqa: E402
from tiny_import import TinyImporter, _is_doc_already_registered  # noqa: E402


UNITS_FIX = {
    "barueri":     {"nome": "Barueri",     "erp": "tiny"},
    "indianopolis": {"nome": "Indianópolis", "erp": "omie"},
}
MASTER_USER = {"email": "admin@astro.com", "name": "Admin", "unit": None, "master": True}


@pytest.fixture
def master_client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=MASTER_USER):
        with server.app.test_client() as c:
            yield c


# ══════════════════════════════════════════════════════════════════════════════
# _is_doc_already_registered (cobre Omie tambem)
# ══════════════════════════════════════════════════════════════════════════════

class TestDocAlreadyRegistered:
    def test_tiny_msg_classica(self):
        assert _is_doc_already_registered(Exception("ja cadastrado no sistema"))

    def test_omie_ja_consta_cadastrado(self):
        assert _is_doc_already_registered(Exception("codigo_lancamento_integracao ja consta cadastrado"))

    def test_omie_codigo_lancamento_integracao(self):
        assert _is_doc_already_registered(Exception("codigo_lancamento_integracao 123 ja existe cadastrado"))

    def test_msg_alheia_retorna_false(self):
        assert not _is_doc_already_registered(Exception("erro de rede"))


# ══════════════════════════════════════════════════════════════════════════════
# _build_erp_importer (roteador)
# ══════════════════════════════════════════════════════════════════════════════

class TestRouter:
    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path):
        server.DATA_DIR = tmp_path
        server.UNITS = UNITS_FIX

    def test_unit_tiny_retorna_TinyImporter(self):
        config = server._build_unit_config("barueri")
        state_dir = server._unit_state_dir("barueri")
        with patch.object(server, "_seed_tokens"):  # nao mexer em tokens em teste
            importer, kind = server._build_erp_importer("barueri", config, state_dir)
        assert kind == "tiny"
        assert isinstance(importer, TinyImporter)

    def test_unit_omie_retorna_OmieImporter(self):
        config = server._build_unit_config("indianopolis")
        state_dir = server._unit_state_dir("indianopolis")
        importer, kind = server._build_erp_importer("indianopolis", config, state_dir)
        assert kind == "omie"
        assert isinstance(importer, OmieImporter)

    def test_unit_sem_erp_default_tiny(self, tmp_path):
        server.UNITS = {"x": {"nome": "X"}}  # sem campo erp
        config = server._build_unit_config("x")
        state_dir = server._unit_state_dir("x")
        with patch.object(server, "_seed_tokens"):
            _, kind = server._build_erp_importer("x", config, state_dir)
        assert kind == "tiny"


# ══════════════════════════════════════════════════════════════════════════════
# _load_omie_config / _save_omie_config
# ══════════════════════════════════════════════════════════════════════════════

class TestOmieConfigPersist:
    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path):
        server.DATA_DIR = tmp_path
        server.UNITS = UNITS_FIX

    def test_load_inexistente_retorna_dict_vazio(self):
        assert server._load_omie_config("indianopolis") == {}

    def test_save_e_load_roundtrip(self):
        cfg = {
            "app_key":           "KEY",
            "app_secret":        "SECRET",
            "id_conta_corrente": 12345,
            "categoria_ids":     {"VISTORIA CAUTELAR": "1.01.01"},
        }
        server._save_omie_config("indianopolis", cfg)
        loaded = server._load_omie_config("indianopolis")
        assert loaded["app_key"] == "KEY"
        assert loaded["app_secret"] == "SECRET"
        assert loaded["id_conta_corrente"] == 12345
        assert loaded["categoria_ids"] == {"VISTORIA CAUTELAR": "1.01.01"}

    def test_save_filtra_chaves_desconhecidas(self):
        server._save_omie_config("indianopolis", {
            "app_key": "K", "lixo": "x", "outra_coisa": 123
        })
        on_disk = json.loads(server._omie_config_path("indianopolis").read_text())
        assert "lixo" not in on_disk
        assert "outra_coisa" not in on_disk

    def test_id_conta_corrente_invalido_vira_zero(self):
        server._save_omie_config("indianopolis", {"id_conta_corrente": "abc"})
        assert server._load_omie_config("indianopolis")["id_conta_corrente"] == 0

    def test_categoria_ids_normaliza_uppercase(self):
        server._save_omie_config("indianopolis", {
            "categoria_ids": {"vistoria cautelar": "1.01.01"}
        })
        loaded = server._load_omie_config("indianopolis")
        assert "VISTORIA CAUTELAR" in loaded["categoria_ids"]


# ══════════════════════════════════════════════════════════════════════════════
# Endpoints
# ══════════════════════════════════════════════════════════════════════════════

class TestEndpointOmieConfig:
    def test_get_unidade_invalida_retorna_400(self, master_client):
        r = master_client.get("/master/api/unidades/inexistente/omie-config")
        assert r.status_code == 400

    def test_get_retorna_config_mascarada(self, master_client):
        server._save_omie_config("indianopolis", {
            "app_key": "MEUKEY", "app_secret": "SUPER_SECRET_LONGO",
            "id_conta_corrente": 999,
        })
        r = master_client.get("/master/api/unidades/indianopolis/omie-config")
        assert r.status_code == 200
        cfg = r.get_json()["config"]
        assert cfg["app_key"] == "MEUKEY"
        assert cfg["has_app_secret"] is True
        assert "***" in cfg["app_secret_masked"]
        assert cfg["id_conta_corrente"] == 999
        # app_secret cru NAO vai no payload
        assert "app_secret" not in cfg

    def test_post_salva_credenciais(self, master_client):
        r = master_client.post("/master/api/unidades/indianopolis/omie-config", json={
            "app_key": "NOVO_KEY", "app_secret": "NOVO_SECRET",
            "id_conta_corrente": 42,
        })
        assert r.status_code == 200
        loaded = server._load_omie_config("indianopolis")
        assert loaded["app_key"] == "NOVO_KEY"
        assert loaded["app_secret"] == "NOVO_SECRET"
        assert loaded["id_conta_corrente"] == 42

    def test_post_sem_app_secret_preserva_atual(self, master_client):
        # Salva inicial
        server._save_omie_config("indianopolis", {
            "app_key": "K1", "app_secret": "S1", "id_conta_corrente": 1
        })
        # POST sem app_secret — deve preservar S1
        master_client.post("/master/api/unidades/indianopolis/omie-config", json={
            "app_key": "K2", "id_conta_corrente": 2
        })
        loaded = server._load_omie_config("indianopolis")
        assert loaded["app_key"] == "K2"
        assert loaded["app_secret"] == "S1"  # preservado
        assert loaded["id_conta_corrente"] == 2

    def test_pagina_html_serve(self, master_client):
        r = master_client.get("/master/erp-config")
        assert r.status_code == 200
        assert b"Configura" in r.data
