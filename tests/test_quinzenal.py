"""
Testes da regra quinzenal de vencimento por cliente.

Bug solicitado pelo Ian: cliente KAVAK fecha quinzenal.
Regra A:
  - Vistoria 1-15 → vence dia 15 do mesmo mês
  - Vistoria 16-fim → vence último dia do mês
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
from tiny_import import (  # noqa: E402
    NormalizedRecord,
    TinyImporter,
    vencimento_quinzenal,
)
from omie_import import OmieImporter  # noqa: E402


UNITS_FIX = {"sp": {"nome": "São Paulo"}}
MASTER_USER = {"email": "admin@astro.com", "name": "Admin", "unit": None, "master": True}
MATRIZ_USER = {"email": "matriz@astro.com", "name": "Matriz", "unit": None, "master": False, "matriz": True}
OP_USER     = {"email": "op@astro.com",     "name": "Op",     "unit": "sp", "master": False}


@pytest.fixture
def master_client(tmp_path):
    server.DATA_DIR = tmp_path
    server._CLIENTES_VENC_FILE = tmp_path / "clientes_vencimento.json"
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=MASTER_USER):
        with server.app.test_client() as c:
            yield c


@pytest.fixture
def op_client(tmp_path):
    server.DATA_DIR = tmp_path
    server._CLIENTES_VENC_FILE = tmp_path / "clientes_vencimento.json"
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=OP_USER):
        with server.app.test_client() as c:
            yield c


# ══════════════════════════════════════════════════════════════════════════════
# vencimento_quinzenal — helper puro
# ══════════════════════════════════════════════════════════════════════════════

class TestVencimentoQuinzenalHelper:
    @pytest.mark.parametrize("data,esperado", [
        ("2026-05-01", "2026-05-15"),
        ("2026-05-15", "2026-05-15"),  # exatamente dia 15 → vence dia 15
        ("2026-05-16", "2026-05-31"),
        ("2026-05-31", "2026-05-31"),
        ("2026-02-10", "2026-02-15"),
        ("2026-02-20", "2026-02-28"),  # fevereiro nao bissexto
        ("2024-02-20", "2024-02-29"),  # 2024 bissexto
        ("2026-12-25", "2026-12-31"),
    ])
    def test_calcula_correto(self, data, esperado):
        assert vencimento_quinzenal(data) == esperado

    def test_aceita_formato_br(self):
        # "06/05/2026" → "2026-05-15"
        assert vencimento_quinzenal("06/05/2026") == "2026-05-15"

    def test_data_invalida_retorna_input(self):
        # Falha silenciosa — caller decide
        assert vencimento_quinzenal("nao-eh-data") == "nao-eh-data"


# ══════════════════════════════════════════════════════════════════════════════
# _modo_vencimento_cliente — match por substring
# ══════════════════════════════════════════════════════════════════════════════

class TestModoVencimentoCliente:
    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path):
        server._CLIENTES_VENC_FILE = tmp_path / "clientes_vencimento.json"
        server._save_clientes_vencimento({"quinzenal": ["KAVAK"]})

    def test_cliente_kavak_eh_quinzenal(self):
        assert server._modo_vencimento_cliente("KAVAK") == "quinzenal"

    def test_substring_match(self):
        # "KAVAK BRASIL LTDA" contem "KAVAK" → quinzenal
        assert server._modo_vencimento_cliente("KAVAK BRASIL LTDA") == "quinzenal"
        assert server._modo_vencimento_cliente("Kavak Soluções") == "quinzenal"
        assert server._modo_vencimento_cliente("kavak transporte") == "quinzenal"

    def test_cliente_sem_kavak_eh_none(self):
        assert server._modo_vencimento_cliente("FULANO LTDA") is None

    def test_cliente_vazio_retorna_none(self):
        assert server._modo_vencimento_cliente("") is None

    def test_lista_vazia_retorna_none(self, tmp_path):
        server._CLIENTES_VENC_FILE = tmp_path / "clientes_vencimento.json"
        server._save_clientes_vencimento({"quinzenal": []})
        assert server._modo_vencimento_cliente("KAVAK") is None


# ══════════════════════════════════════════════════════════════════════════════
# Storage save/load
# ══════════════════════════════════════════════════════════════════════════════

class TestStorage:
    def test_load_inexistente_retorna_lista_vazia(self, tmp_path):
        server._CLIENTES_VENC_FILE = tmp_path / "clientes_vencimento.json"
        assert server._load_clientes_vencimento() == {"quinzenal": []}

    def test_save_e_load(self, tmp_path):
        server._CLIENTES_VENC_FILE = tmp_path / "clientes_vencimento.json"
        server._save_clientes_vencimento({"quinzenal": ["KAVAK", "FULANO"]})
        loaded = server._load_clientes_vencimento()
        assert set(loaded["quinzenal"]) == {"KAVAK", "FULANO"}

    def test_save_normaliza_uppercase(self, tmp_path):
        server._CLIENTES_VENC_FILE = tmp_path / "clientes_vencimento.json"
        server._save_clientes_vencimento({"quinzenal": ["kavak"]})
        assert "KAVAK" in server._load_clientes_vencimento()["quinzenal"]

    def test_save_dedup(self, tmp_path):
        server._CLIENTES_VENC_FILE = tmp_path / "clientes_vencimento.json"
        server._save_clientes_vencimento({"quinzenal": ["KAVAK", "kavak", "Kavak"]})
        assert server._load_clientes_vencimento()["quinzenal"].count("KAVAK") == 1


# ══════════════════════════════════════════════════════════════════════════════
# Integração Tiny — vencimento aplica regra quinzenal
# ══════════════════════════════════════════════════════════════════════════════

def _make_rec(cliente="KAVAK BRASIL", data="2026-05-06", fp="FA", av_pagamento="") -> NormalizedRecord:
    return NormalizedRecord(
        data=data, modelo="GOL", placa="ABC1234", cliente=cliente,
        servico="VISTORIA CAUTELAR", fp=fp, preco="100.00",
        origem_arquivo="t.xls", linha_origem=1,
        chave_deduplicacao="x", av_pagamento=av_pagamento, cpf="",
    )


def _make_tiny_config(modo_fn=None) -> dict:
    cfg = {
        "tiny": {
            "base_url": "https://api.tiny.com.br/public-api/v3",
            "token_url": "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token",
            "oauth_scope": "openid",
            "timeout_seconds": 30,
            "client_id": "test", "client_secret": "test",
            "redirect_uri": "http://localhost/cb", "scope": "openid",
            "cliente_ids": {}, "forma_recebimento_ids": {"FA": 200},
            "categoria_ids": {}, "auto_create_contacts": False,
            "include_forma_recebimento": True,
            "numero_documento_prefix": "PLANILHA", "default_tipo_pessoa": "J",
            "require_payment_mapping": False, "vencimento_tipo": "ultimo_dia_mes",
            "vencimento_dias": 0, "contas_receber_fp": ["FA"],
            "servico_aliases": {}, "fp_aliases": {}, "cliente_aliases": {},
        }
    }
    if modo_fn:
        cfg["tiny"]["vencimento_modo_cliente_fn"] = modo_fn
    return cfg


class TestTinyComQuinzenal:
    def _patch_resolvers(self, importer):
        importer.resolve_contact = lambda *a, **k: 999
        importer.resolve_payment = lambda *a, **k: 200

    def test_cliente_quinzenal_dia_06_vence_dia_15(self, tmp_path):
        importer = TinyImporter(_make_tiny_config(modo_fn=lambda n: "quinzenal"), tmp_path)
        self._patch_resolvers(importer)
        rec = _make_rec(cliente="KAVAK", data="2026-05-06")
        payload = importer.build_accounts_receivable_payload(rec)
        assert payload["dataVencimento"] == "2026-05-15"

    def test_cliente_quinzenal_dia_20_vence_dia_31(self, tmp_path):
        importer = TinyImporter(_make_tiny_config(modo_fn=lambda n: "quinzenal"), tmp_path)
        self._patch_resolvers(importer)
        rec = _make_rec(cliente="KAVAK", data="2026-05-20")
        payload = importer.build_accounts_receivable_payload(rec)
        assert payload["dataVencimento"] == "2026-05-31"

    def test_cliente_nao_quinzenal_vence_fim_mes(self, tmp_path):
        importer = TinyImporter(_make_tiny_config(modo_fn=lambda n: None), tmp_path)
        self._patch_resolvers(importer)
        rec = _make_rec(cliente="FULANO LTDA", data="2026-05-06")
        payload = importer.build_accounts_receivable_payload(rec)
        # Sem regra especial → ultimo dia do mes
        assert payload["dataVencimento"] == "2026-05-31"

    def test_sem_callback_fallback_ultimo_dia(self, tmp_path):
        """Importer construido sem callback → cai no comportamento padrao."""
        importer = TinyImporter(_make_tiny_config(modo_fn=None), tmp_path)
        self._patch_resolvers(importer)
        rec = _make_rec(cliente="KAVAK", data="2026-05-06")
        payload = importer.build_accounts_receivable_payload(rec)
        # Sem callback → KAVAK trata como FA padrao
        assert payload["dataVencimento"] == "2026-05-31"


# ══════════════════════════════════════════════════════════════════════════════
# Endpoints
# ══════════════════════════════════════════════════════════════════════════════

class TestEndpoints:
    def test_get_sem_config_retorna_vazio(self, master_client):
        body = master_client.get("/master/api/clientes-vencimento").get_json()
        assert body["success"] is True
        assert body["config"]["quinzenal"] == []

    def test_post_master_salva(self, master_client):
        r = master_client.post("/master/api/clientes-vencimento",
                               json={"quinzenal": ["KAVAK"]})
        assert r.status_code == 200
        body = r.get_json()
        assert "KAVAK" in body["config"]["quinzenal"]

    def test_post_operador_recebe_403(self, op_client):
        r = op_client.post("/master/api/clientes-vencimento",
                           json={"quinzenal": ["KAVAK"]})
        assert r.status_code == 403

    def test_pagina_html_serve(self, master_client):
        r = master_client.get("/master/clientes-vencimento")
        assert r.status_code == 200
        assert b"Quinzenal" in r.data or b"quinzenal" in r.data
