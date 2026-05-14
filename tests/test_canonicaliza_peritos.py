"""
Testes do merge de peritos truncados em tamanhos diferentes.

Bug operacional 2026-05-13: Sispevi trunca nome a 19 chars, Megalaudo a 20.
Resultado: "EDMILSON APARECIDO" e "EDMILSON APARECIDO N" aparecem como 2
peritos no ranking, mas sao a mesma pessoa.

Heuristica: dois nomes sao o mesmo perito se um eh prefixo do outro com
pelo menos 15 chars iguais. Mais longo vira canonico.
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


@pytest.fixture
def setup(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    yield tmp_path


@pytest.fixture
def master_client(setup):
    with patch.object(server, "_current_user", return_value=MASTER_USER):
        with server.app.test_client() as c:
            yield c


# ══════════════════════════════════════════════════════════════════════════════
# _canonicaliza_peritos_map (unidade pura)
# ══════════════════════════════════════════════════════════════════════════════

class TestMapBasico:
    def test_caso_screenshot_edmilson(self):
        m = server._canonicaliza_peritos_map({
            "EDMILSON APARECIDO",
            "EDMILSON APARECIDO N",
        })
        # Mais longo vira canonico, curto aponta pra ele
        assert m["EDMILSON APARECIDO"]   == "EDMILSON APARECIDO N"
        assert m["EDMILSON APARECIDO N"] == "EDMILSON APARECIDO N"

    def test_caso_screenshot_diego(self):
        m = server._canonicaliza_peritos_map({
            "DIEGO CARVALHO GONÇ",
            "DIEGO CARVALHO GONÇA",
        })
        assert m["DIEGO CARVALHO GONÇ"]  == "DIEGO CARVALHO GONÇA"
        assert m["DIEGO CARVALHO GONÇA"] == "DIEGO CARVALHO GONÇA"

    def test_nome_sozinho_eh_proprio_canonico(self):
        m = server._canonicaliza_peritos_map({"VICTOR CRECHI DA SI"})
        assert m["VICTOR CRECHI DA SI"] == "VICTOR CRECHI DA SI"

    def test_tres_tamanhos_todos_apontam_pro_maior(self):
        """Truncamentos progressivos do mesmo nome — todos pro maior."""
        m = server._canonicaliza_peritos_map({
            "JOAO SILVA SANTOS",       # 17
            "JOAO SILVA SANTOS NETO",  # 22
            "JOAO SILVA SANTOS",       # idem
        })
        canon = m["JOAO SILVA SANTOS"]
        assert canon == "JOAO SILVA SANTOS NETO"
        assert m["JOAO SILVA SANTOS NETO"] == "JOAO SILVA SANTOS NETO"

    def test_nomes_diferentes_nao_mergeiam(self):
        m = server._canonicaliza_peritos_map({
            "EDMILSON APARECIDO",
            "FERNANDO JHONATAN D",
        })
        assert m["EDMILSON APARECIDO"]   == "EDMILSON APARECIDO"
        assert m["FERNANDO JHONATAN D"]  == "FERNANDO JHONATAN D"

    def test_prefixo_curto_nao_mergeia(self):
        """Threshold de 15 chars — nomes curtos NAO mergeiam mesmo com prefixo."""
        m = server._canonicaliza_peritos_map({
            "JOSE SILVA",         # 10 — curto demais
            "JOSE SILVA SANTOS",  # 17
        })
        # 10 chars < 15 → nao mergeia
        assert m["JOSE SILVA"]        == "JOSE SILVA"
        assert m["JOSE SILVA SANTOS"] == "JOSE SILVA SANTOS"

    def test_diferenca_no_meio_nao_mergeia(self):
        """Mesmo com nomes longos, se divergem antes de 15 chars, nao mergeia."""
        m = server._canonicaliza_peritos_map({
            "EDMILSON APARECIDA NUNES",   # mulher
            "EDMILSON APARECIDO NOGUEIRA",  # homem
        })
        # Divergem no char 11 (A vs O) — apesar de longos, sao pessoas diferentes
        # Espera: 11 chars iguais < 15 → nao mergeia
        assert m["EDMILSON APARECIDA NUNES"]    == "EDMILSON APARECIDA NUNES"
        assert m["EDMILSON APARECIDO NOGUEIRA"] == "EDMILSON APARECIDO NOGUEIRA"

    def test_vazio(self):
        assert server._canonicaliza_peritos_map(set()) == {}
        assert server._canonicaliza_peritos_map({""}) == {}


# ══════════════════════════════════════════════════════════════════════════════
# Integracao com _agrega_vistorias_por_perito
# ══════════════════════════════════════════════════════════════════════════════

def _v(perito, n=1, placa_prefix="A", servico="VISTORIA CAUTELAR", data="2026-05-13"):
    return [
        {"data": data, "placa": f"{placa_prefix}{i:04d}", "cliente": "X",
         "servico": servico, "valor": 100.0, "fp": "AV", "perito": perito}
        for i in range(n)
    ]


class TestAgregaComMerge:
    def test_mescla_truncamentos_no_agregado(self):
        rows = _v("EDMILSON APARECIDO", n=6, placa_prefix="A") + \
               _v("EDMILSON APARECIDO N", n=2, placa_prefix="B")
        out = server._agrega_vistorias_por_perito(rows)
        # 1 perito so, com 8 vistorias somadas
        assert len(out) == 1
        assert out[0]["perito"] == "EDMILSON APARECIDO N"
        assert out[0]["qtd"]    == 8
        assert out[0]["valor"]  == 800.0

    def test_categoria_premio_consolida(self):
        """Quebra de categoria_premio tambem consolida no canonico."""
        rows = _v("DIEGO CARVALHO GONÇ", n=3, placa_prefix="A", servico="VISTORIA CAUTELAR") + \
               _v("DIEGO CARVALHO GONÇA", n=2, placa_prefix="B", servico="CAUTELAR + PINTURA")
        out = server._agrega_vistorias_por_perito(rows)
        assert len(out) == 1
        cats = out[0]["premio"]["premio_por_categoria"]
        assert cats["cautelar"]["qtd"]          == 3
        assert cats["cautelar_pintura"]["qtd"]  == 2


# ══════════════════════════════════════════════════════════════════════════════
# End-to-end via endpoint /api/relatorio/vistoriadores
# ══════════════════════════════════════════════════════════════════════════════

class TestEndpointComMerge:
    def test_endpoint_consolida_truncamentos(self, master_client, setup):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        # Simula 6 vistorias importadas via Megalaudo (truncado a 18) e
        # 2 via Sispevi (truncado a 20) — mesmo perito
        vistorias = _v("EDMILSON APARECIDO", n=6, placa_prefix="M") + \
                    _v("EDMILSON APARECIDO N", n=2, placa_prefix="S")
        upsert_vistorias_planilha("sp", unit_dir, vistorias)

        r = master_client.get(
            "/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31&unit=sp"
        )
        body = r.get_json()
        # 1 perito so no ranking, nao 2
        nomes = [p["perito"] for p in body["peritos"]]
        assert nomes == ["EDMILSON APARECIDO N"]
        assert body["peritos"][0]["qtd"] == 8

    def test_peritos_distintos_continuam_distintos(self, master_client, setup):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        vistorias = _v("EDMILSON APARECIDO", n=5, placa_prefix="E") + \
                    _v("DIEGO CARVALHO GONÇ", n=3, placa_prefix="D")
        upsert_vistorias_planilha("sp", unit_dir, vistorias)

        r = master_client.get(
            "/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31&unit=sp"
        )
        nomes = {p["perito"] for p in r.get_json()["peritos"]}
        assert len(nomes) == 2
