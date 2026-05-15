"""
Testes das sugestoes automaticas de merge de peritos.

Sistema detecta pares de nomes parecidos (primeiro+segundo nome iguais)
que talvez sejam o mesmo perito truncado de formas diferentes, mas que
o automerge (threshold 15 chars) nao pegou. Master decide caso a caso.

Caso real (Ian, screenshot 2026-05-13): "ELTON FLAVIO OLIVEIR" vs
"ELTON FLAVIO FERNAN" — primeiros 12 chars iguais, mas divergem antes
do threshold de automerge.
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
from caixa_db import upsert_vistorias_planilha  # noqa: E402


UNITS_FIX = {"sp": {"nome": "São Paulo"}}
MASTER_USER = {"email": "m@a.com", "name": "M", "unit": None, "master": True}
OP_USER     = {"email": "o@a.com", "name": "O", "unit": "sp", "master": False}


@pytest.fixture
def setup(tmp_path):
    server.DATA_DIR = tmp_path
    server._PERITOS_ALIASES_FILE   = tmp_path / "peritos_aliases.json"
    server._PERITOS_DISTINTOS_FILE = tmp_path / "peritos_distintos.json"
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    yield tmp_path


@pytest.fixture
def master_client(setup):
    with patch.object(server, "_current_user", return_value=MASTER_USER):
        with server.app.test_client() as c:
            yield c


@pytest.fixture
def op_client(setup):
    with patch.object(server, "_current_user", return_value=OP_USER):
        with server.app.test_client() as c:
            yield c


# ══════════════════════════════════════════════════════════════════════════════
# _palavras_em_comum_inicio
# ══════════════════════════════════════════════════════════════════════════════

class TestPalavrasComum:
    def test_2_palavras(self):
        assert server._palavras_em_comum_inicio(
            "ELTON FLAVIO OLIVEIRA", "ELTON FLAVIO FERNANDES"
        ) == 2

    def test_3_palavras(self):
        assert server._palavras_em_comum_inicio(
            "JOSE DA SILVA SANTOS", "JOSE DA SILVA PEREIRA"
        ) == 3

    def test_1_palavra(self):
        assert server._palavras_em_comum_inicio(
            "JOAO SILVA", "JOAO PEREIRA"
        ) == 1

    def test_zero_palavras(self):
        assert server._palavras_em_comum_inicio(
            "ANA SILVA", "BETO SILVA"
        ) == 0


# ══════════════════════════════════════════════════════════════════════════════
# _sugestoes_merge_peritos (unidade)
# ══════════════════════════════════════════════════════════════════════════════

class TestSugestoesUnidade:
    def test_caso_elton_flavio(self):
        """Sistema sugere par com primeiros 12 chars iguais."""
        s = server._sugestoes_merge_peritos(
            nomes={"ELTON FLAVIO OLIVEIR", "ELTON FLAVIO FERNAN"},
            ja_aliases={},
            distintos=set(),
            contagens={"ELTON FLAVIO OLIVEIR": 4, "ELTON FLAVIO FERNAN": 2},
        )
        assert len(s) == 1
        assert {s[0]["nome_a"], s[0]["nome_b"]} == {
            "ELTON FLAVIO OLIVEIR", "ELTON FLAVIO FERNAN"
        }
        assert s[0]["qtd_a"] + s[0]["qtd_b"] == 6

    def test_prefixo_curto_nao_sugere(self):
        """JOAO SILVA (10 chars) ainda atinge o piso de 10 — sugere.
        Mas JOAO (4 chars) nao."""
        s = server._sugestoes_merge_peritos(
            nomes={"JOAO MARCOS", "JOAO PAULO"},  # 5 chars iguais
            ja_aliases={}, distintos=set(),
        )
        assert len(s) == 0  # so 5 chars comum < 10

    def test_acima_de_15_eh_automerge_nao_sugere(self):
        """Prefixo >= 15 ja eh tratado pelo automerge — nao deve aparecer aqui."""
        s = server._sugestoes_merge_peritos(
            nomes={"EDMILSON APARECIDO", "EDMILSON APARECIDO N"},  # >=15 chars iguais
            ja_aliases={}, distintos=set(),
        )
        assert len(s) == 0

    def test_minimo_2_palavras(self):
        """1 palavra em comum nao sugere mesmo com 10+ chars de prefixo."""
        s = server._sugestoes_merge_peritos(
            nomes={"BERNARDO ANDRADE", "BERNARDINHO LIMA"},  # 10 chars, 1 palavra
            ja_aliases={}, distintos=set(),
        )
        assert len(s) == 0

    def test_dispensados_nao_aparecem(self):
        s = server._sugestoes_merge_peritos(
            nomes={"ELTON FLAVIO OLIVEIR", "ELTON FLAVIO FERNAN"},
            ja_aliases={},
            distintos={tuple(sorted(["ELTON FLAVIO OLIVEIR", "ELTON FLAVIO FERNAN"]))},
        )
        assert len(s) == 0

    def test_ja_aliased_nao_aparece(self):
        """Se um aponta pro outro via alias, nao sugere."""
        s = server._sugestoes_merge_peritos(
            nomes={"ELTON FLAVIO OLIVEIR", "ELTON FLAVIO FERNAN"},
            ja_aliases={
                "ELTON FLAVIO OLIVEIR": "ELTON FLAVIO COMPLETO",
                "ELTON FLAVIO FERNAN":  "ELTON FLAVIO COMPLETO",
            },
            distintos=set(),
        )
        # Ambos ja apontam pro mesmo canonico — nao sugere
        assert len(s) == 0

    def test_ordena_por_total_de_vistorias_desc(self):
        """Prefixo intencionalmente <15 chars (range de sugestao, nao automerge)."""
        s = server._sugestoes_merge_peritos(
            nomes={
                # "ANA COSTA" = 9; com "OLI"/"SAN" no fim eh prefixo 10 (palavra 2 = COSTA)
                "ANA COSTA OLI", "ANA COSTA SAN",      # 2 vistorias total, 10 chars prefix
                "BENI LIMA OLI", "BENI LIMA SAN",      # 20 vistorias, 10 chars prefix
            },
            ja_aliases={}, distintos=set(),
            contagens={
                "ANA COSTA OLI": 1,   "ANA COSTA SAN": 1,
                "BENI LIMA OLI": 15,  "BENI LIMA SAN": 5,
            },
        )
        assert len(s) == 2
        # BENI (20 vistorias) vem antes de ANA (2)
        assert s[0]["nome_a"].startswith("BENI")
        assert s[1]["nome_a"].startswith("ANA")


# ══════════════════════════════════════════════════════════════════════════════
# Storage de distintos
# ══════════════════════════════════════════════════════════════════════════════

class TestStorageDistintos:
    def test_load_vazio(self, setup):
        assert server._load_peritos_distintos() == set()

    def test_save_e_load(self, setup):
        server._save_peritos_distintos({("A", "B"), ("C", "D")})
        loaded = server._load_peritos_distintos()
        assert ("A", "B") in loaded or ("B", "A") in loaded
        assert ("C", "D") in loaded or ("D", "C") in loaded

    def test_par_ordenado_dedup_automatico(self, setup):
        server._save_peritos_distintos({("B", "A"), ("A", "B")})
        loaded = server._load_peritos_distintos()
        # Apenas 1 par (ordenado A,B)
        assert len(loaded) == 1
        assert ("A", "B") in loaded


# ══════════════════════════════════════════════════════════════════════════════
# Endpoint /master/api/peritos-aliases/sugestoes
# ══════════════════════════════════════════════════════════════════════════════

class TestEndpointSugestoes:
    def test_get_vazio_sem_dados(self, master_client):
        r = master_client.get("/master/api/peritos-aliases/sugestoes")
        body = r.get_json()
        assert body["success"]
        assert body["sugestoes"] == []

    def test_get_sugere_par_real(self, master_client, setup):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        import datetime as dt
        hoje = dt.date.today().isoformat()
        upsert_vistorias_planilha("sp", unit_dir, [
            {"data": hoje, "placa": f"A{i:04d}", "cliente": "X",
             "servico": "VISTORIA CAUTELAR", "valor": 100, "fp": "AV",
             "perito": "ELTON FLAVIO OLIVEIR"} for i in range(4)
        ] + [
            {"data": hoje, "placa": f"B{i:04d}", "cliente": "Y",
             "servico": "VISTORIA CAUTELAR", "valor": 100, "fp": "AV",
             "perito": "ELTON FLAVIO FERNAN"} for i in range(2)
        ])
        r = master_client.get("/master/api/peritos-aliases/sugestoes")
        body = r.get_json()
        assert len(body["sugestoes"]) == 1
        s = body["sugestoes"][0]
        assert {s["nome_a"], s["nome_b"]} == {
            "ELTON FLAVIO OLIVEIR", "ELTON FLAVIO FERNAN"
        }


# ══════════════════════════════════════════════════════════════════════════════
# Endpoint /master/api/peritos-aliases/dispensar
# ══════════════════════════════════════════════════════════════════════════════

class TestEndpointDispensar:
    def test_master_dispensa_par(self, master_client, setup):
        r = master_client.post("/master/api/peritos-aliases/dispensar", json={
            "nome_a": "ELTON FLAVIO OLIVEIR",
            "nome_b": "ELTON FLAVIO FERNAN",
        })
        assert r.status_code == 200
        assert r.get_json()["total"] == 1
        # Confirma persistencia
        distintos = server._load_peritos_distintos()
        assert ("ELTON FLAVIO FERNAN", "ELTON FLAVIO OLIVEIR") in distintos

    def test_dispensar_dedup(self, master_client, setup):
        master_client.post("/master/api/peritos-aliases/dispensar", json={
            "nome_a": "A", "nome_b": "B"
        })
        # Mesmo par ordem reversa nao duplica
        master_client.post("/master/api/peritos-aliases/dispensar", json={
            "nome_a": "B", "nome_b": "A"
        })
        assert len(server._load_peritos_distintos()) == 1

    def test_operador_403(self, op_client):
        r = op_client.post("/master/api/peritos-aliases/dispensar", json={
            "nome_a": "A", "nome_b": "B"
        })
        assert r.status_code == 403

    def test_invalido_400(self, master_client):
        r = master_client.post("/master/api/peritos-aliases/dispensar", json={
            "nome_a": "", "nome_b": "B"
        })
        assert r.status_code == 400

    def test_iguais_400(self, master_client):
        r = master_client.post("/master/api/peritos-aliases/dispensar", json={
            "nome_a": "A", "nome_b": "A"
        })
        assert r.status_code == 400


# ══════════════════════════════════════════════════════════════════════════════
# Integracao: dispensar depois sugestoes nao retorna o par
# ══════════════════════════════════════════════════════════════════════════════

class TestIntegracao:
    def test_dispensar_remove_de_sugestoes(self, master_client, setup):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        import datetime as dt
        hoje = dt.date.today().isoformat()
        upsert_vistorias_planilha("sp", unit_dir, [
            {"data": hoje, "placa": f"A{i:04d}", "cliente": "X",
             "servico": "VISTORIA CAUTELAR", "valor": 100, "fp": "AV",
             "perito": "ELTON FLAVIO OLIVEIR"} for i in range(3)
        ] + [
            {"data": hoje, "placa": f"B{i:04d}", "cliente": "Y",
             "servico": "VISTORIA CAUTELAR", "valor": 100, "fp": "AV",
             "perito": "ELTON FLAVIO FERNAN"} for i in range(2)
        ])
        # Antes: aparece como sugestao
        r1 = master_client.get("/master/api/peritos-aliases/sugestoes")
        assert len(r1.get_json()["sugestoes"]) == 1

        # Dispensa
        master_client.post("/master/api/peritos-aliases/dispensar", json={
            "nome_a": "ELTON FLAVIO OLIVEIR",
            "nome_b": "ELTON FLAVIO FERNAN",
        })

        # Depois: nao aparece mais
        r2 = master_client.get("/master/api/peritos-aliases/sugestoes")
        assert r2.get_json()["sugestoes"] == []
