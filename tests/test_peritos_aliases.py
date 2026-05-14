"""
Testes dos aliases manuais de peritos.

Quando o automerge automatico nao resolve (ex: "VICTOR CRECHI DA SI" sem
versao mais longa na base), o master cadastra via UI o nome completo.
Aliases tem prioridade sobre automerge.
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
from caixa_db import upsert_vistorias_planilha  # noqa: E402


UNITS_FIX = {"sp": {"nome": "São Paulo"}}
MASTER_USER = {"email": "m@a.com", "name": "M", "unit": None, "master": True}
MATRIZ_USER = {"email": "x@a.com", "name": "X", "unit": None, "master": False, "matriz": True}
OP_USER     = {"email": "o@a.com", "name": "O", "unit": "sp", "master": False}


@pytest.fixture
def setup(tmp_path):
    server.DATA_DIR = tmp_path
    server._PERITOS_ALIASES_FILE = tmp_path / "peritos_aliases.json"
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    yield tmp_path


@pytest.fixture
def master_client(setup):
    with patch.object(server, "_current_user", return_value=MASTER_USER):
        with server.app.test_client() as c:
            yield c


@pytest.fixture
def matriz_client(setup):
    with patch.object(server, "_current_user", return_value=MATRIZ_USER):
        with server.app.test_client() as c:
            yield c


@pytest.fixture
def op_client(setup):
    with patch.object(server, "_current_user", return_value=OP_USER):
        with server.app.test_client() as c:
            yield c


# ══════════════════════════════════════════════════════════════════════════════
# Storage
# ══════════════════════════════════════════════════════════════════════════════

class TestStorage:
    def test_load_inexistente(self, setup):
        assert server._load_peritos_aliases() == {}

    def test_save_e_load(self, setup):
        server._save_peritos_aliases({"VICTOR CRECHI DA SI": "VICTOR CRECHI DA SILVA"})
        assert server._load_peritos_aliases() == {
            "VICTOR CRECHI DA SI": "VICTOR CRECHI DA SILVA"
        }

    def test_save_normaliza_uppercase(self, setup):
        server._save_peritos_aliases({"victor crechi da si": "Victor Crechi da Silva"})
        assert server._load_peritos_aliases() == {
            "VICTOR CRECHI DA SI": "VICTOR CRECHI DA SILVA"
        }

    def test_save_descarta_invalidos(self, setup):
        server._save_peritos_aliases({
            "":             "X",         # chave vazia
            "Y":            "",          # valor vazio
            "ABC":          "ABC",       # iguais (no-op)
            "VICTOR CR":    "VICTOR CRECHI DA SILVA",
        })
        a = server._load_peritos_aliases()
        assert a == {"VICTOR CR": "VICTOR CRECHI DA SILVA"}

    def test_load_arquivo_corrompido_retorna_vazio(self, setup):
        server._PERITOS_ALIASES_FILE.write_text("nao eh json valido", encoding="utf-8")
        assert server._load_peritos_aliases() == {}


# ══════════════════════════════════════════════════════════════════════════════
# Integracao com _canonicaliza_peritos_map
# ══════════════════════════════════════════════════════════════════════════════

class TestCanonicalizaComAliases:
    def test_alias_aplica_pra_truncado_sem_par_longo(self):
        """Caso classico: 'VICTOR CRECHI DA SI' sozinho na base."""
        m = server._canonicaliza_peritos_map(
            {"VICTOR CRECHI DA SI"},
            manual_aliases={"VICTOR CRECHI DA SI": "VICTOR CRECHI DA SILVA"},
        )
        assert m["VICTOR CRECHI DA SI"] == "VICTOR CRECHI DA SILVA"

    def test_alias_redireciona_canonico_do_automerge(self):
        """Automerge mergeia 2 truncados, mas alias diz o nome completo real."""
        m = server._canonicaliza_peritos_map(
            {"EDMILSON APARECIDO", "EDMILSON APARECIDO N"},
            manual_aliases={"EDMILSON APARECIDO N": "EDMILSON APARECIDO NOGUEIRA"},
        )
        # Automerge: "EDMILSON APARECIDO" → "EDMILSON APARECIDO N"
        # Alias: "EDMILSON APARECIDO N" → "EDMILSON APARECIDO NOGUEIRA"
        # Resultado final: ambos → "EDMILSON APARECIDO NOGUEIRA"
        assert m["EDMILSON APARECIDO"]   == "EDMILSON APARECIDO NOGUEIRA"
        assert m["EDMILSON APARECIDO N"] == "EDMILSON APARECIDO NOGUEIRA"

    def test_alias_chain_resolve(self):
        """A → B, B → C deve resolver A → C."""
        m = server._canonicaliza_peritos_map(
            {"A NAME"},
            manual_aliases={
                "A NAME": "B NAME",
                "B NAME": "C NAME",
            },
        )
        assert m["A NAME"] == "C NAME"

    def test_sem_aliases_funciona_como_antes(self):
        """Backwards compat: chamada sem aliases mantem comportamento."""
        m = server._canonicaliza_peritos_map({"VICTOR CRECHI DA SI"})
        assert m["VICTOR CRECHI DA SI"] == "VICTOR CRECHI DA SI"

    def test_alias_para_nome_que_nao_aparece(self):
        """Alias cadastrado mas nome truncado nao aparece em rows — nao quebra."""
        m = server._canonicaliza_peritos_map(
            {"OUTRO NOME"},
            manual_aliases={"INEXISTENTE": "TAMBEM INEXISTENTE"},
        )
        assert m["OUTRO NOME"] == "OUTRO NOME"


# ══════════════════════════════════════════════════════════════════════════════
# Endpoints
# ══════════════════════════════════════════════════════════════════════════════

class TestEndpoints:
    def test_get_vazio(self, master_client):
        body = master_client.get("/master/api/peritos-aliases").get_json()
        assert body["success"]
        assert body["aliases"] == {}

    def test_post_master_salva(self, master_client):
        r = master_client.post("/master/api/peritos-aliases", json={
            "aliases": {"VICTOR CRECHI DA SI": "VICTOR CRECHI DA SILVA"}
        })
        assert r.status_code == 200
        body = r.get_json()
        assert body["aliases"]["VICTOR CRECHI DA SI"] == "VICTOR CRECHI DA SILVA"

    def test_post_matriz_salva(self, matriz_client):
        r = matriz_client.post("/master/api/peritos-aliases", json={
            "aliases": {"A": "B"}
        })
        assert r.status_code == 200

    def test_post_operador_403(self, op_client):
        r = op_client.post("/master/api/peritos-aliases", json={"aliases": {}})
        assert r.status_code == 403

    def test_get_inclui_sem_alias(self, master_client, setup):
        """GET retorna lista de peritos em uso que NAO tem alias cadastrado."""
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        import datetime as dt
        hoje = dt.date.today().isoformat()
        upsert_vistorias_planilha("sp", unit_dir, [
            {"data": hoje, "placa": "ABC1234", "cliente": "X",
             "servico": "VISTORIA CAUTELAR", "valor": 100, "fp": "AV",
             "perito": "VICTOR CRECHI DA SI"},
            {"data": hoje, "placa": "XYZ9999", "cliente": "Y",
             "servico": "VISTORIA CAUTELAR", "valor": 100, "fp": "AV",
             "perito": "JOAO COMPLETO"},
        ])
        # Cadastra alias so pra VICTOR
        server._save_peritos_aliases({"VICTOR CRECHI DA SI": "VICTOR CRECHI DA SILVA"})

        body = master_client.get("/master/api/peritos-aliases").get_json()
        assert "VICTOR CRECHI DA SI" in body["em_uso"]
        assert "JOAO COMPLETO"       in body["em_uso"]
        # VICTOR tem alias → nao aparece em sem_alias
        assert "VICTOR CRECHI DA SI" not in body["sem_alias"]
        # JOAO nao tem alias → aparece
        assert "JOAO COMPLETO"       in body["sem_alias"]


# ══════════════════════════════════════════════════════════════════════════════
# End-to-end: alias aplica no /api/relatorio/vistoriadores
# ══════════════════════════════════════════════════════════════════════════════

class TestEndpointRelatorioComAlias:
    def test_relatorio_mostra_nome_canonico(self, master_client, setup):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        upsert_vistorias_planilha("sp", unit_dir, [
            {"data": "2026-05-13", "placa": f"V{i:04d}", "cliente": "X",
             "servico": "VISTORIA CAUTELAR", "valor": 100, "fp": "AV",
             "perito": "VICTOR CRECHI DA SI"}
            for i in range(5)
        ])
        # Sem alias: aparece como "VICTOR CRECHI DA SI"
        r1 = master_client.get(
            "/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31&unit=sp"
        )
        assert r1.get_json()["peritos"][0]["perito"] == "VICTOR CRECHI DA SI"

        # Cadastra alias
        server._save_peritos_aliases({"VICTOR CRECHI DA SI": "VICTOR CRECHI DA SILVA"})

        # Agora aparece com nome completo
        r2 = master_client.get(
            "/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31&unit=sp"
        )
        body = r2.get_json()
        assert body["peritos"][0]["perito"] == "VICTOR CRECHI DA SILVA"
        assert body["peritos"][0]["qtd"]    == 5


# ══════════════════════════════════════════════════════════════════════════════
# Pagina HTML
# ══════════════════════════════════════════════════════════════════════════════

class TestPagina:
    def test_html_serve(self, master_client):
        r = master_client.get("/master/peritos-aliases")
        assert r.status_code == 200
        assert b"Aliases" in r.data

    def test_html_bloqueia_operador(self, op_client):
        r = op_client.get("/master/peritos-aliases")
        assert r.status_code == 403
