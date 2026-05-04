"""
Testes dos aliases globais pra "CAUTELAR + PINTURA" (canonico).

Decisao 04/05/2026: nome canonico vira "CAUTELAR + PINTURA" (18 chars, cabe
no Sispevi que trunca em 19). Moema ja usa esse nome; Barueri/Mooca tinham
"CAUTELAR COM ANALISE DE PINTURA" (truncava).

Variantes legacy/longas que viram canonico via alias:
- "CAUTELAR COM ANALISE DE PINTURA"  (legacy, 31 chars)
- "CAUTELAR COM ANALISE DE PINTURAA" (com 2 As, 32 chars)
- "CAUTELAR COM ANALISE DE PINTUR"   (sem A final, 30 chars)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

os.environ.setdefault("SECRET_KEY", "test-secret-key-pytest")
os.environ.setdefault("USERS_CONFIG", "{}")
os.environ.setdefault("UNITS_CONFIG", "{}")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import server  # noqa: E402
from tiny_import import apply_alias  # noqa: E402


@pytest.fixture
def config_com_aliases():
    """Simula o config que _build_unit_config produz — com aliases globais aplicados."""
    return {
        "tiny": {
            "aliases": {
                "servico": dict(server._GLOBAL_ALIASES["servico"]),
                "fp": {},
                "cliente": {},
            }
        }
    }


class TestAliasCautelarPintura:
    def test_nome_canonico_curto_inalterado(self, config_com_aliases):
        result = apply_alias(config_com_aliases, "servico", "CAUTELAR + PINTURA")
        assert result == "CAUTELAR + PINTURA"

    def test_legacy_longo_normaliza_pra_curto(self, config_com_aliases):
        result = apply_alias(config_com_aliases, "servico", "CAUTELAR COM ANALISE DE PINTURA")
        assert result == "CAUTELAR + PINTURA"

    def test_dois_As_finais_normaliza(self, config_com_aliases):
        result = apply_alias(config_com_aliases, "servico", "CAUTELAR COM ANALISE DE PINTURAA")
        assert result == "CAUTELAR + PINTURA"

    def test_sem_A_final_normaliza(self, config_com_aliases):
        result = apply_alias(config_com_aliases, "servico", "CAUTELAR COM ANALISE DE PINTUR")
        assert result == "CAUTELAR + PINTURA"

    def test_alias_legacy_laudo_cautelar_continua_funcionando(self, config_com_aliases):
        """Garante que nao quebrei o alias antigo 'LAUDO CAUTELAR' → 'VISTORIA CAUTELAR'."""
        result = apply_alias(config_com_aliases, "servico", "LAUDO CAUTELAR")
        assert result == "VISTORIA CAUTELAR"

    def test_servico_desconhecido_passa_sem_mudanca(self, config_com_aliases):
        result = apply_alias(config_com_aliases, "servico", "ALGUM SERVICO NOVO")
        assert result == "ALGUM SERVICO NOVO"


class TestAliasIntegradoComCategoriasUnidade:
    """Confere que Barueri e Mooca tem 'CAUTELAR + PINTURA' no
    _CATEGORIAS_POR_UNIDADE apontando pro mesmo ID que ja tinha em
    'CAUTELAR COM ANALISE DE PINTURA' (legacy mantido pra compat)."""

    def test_barueri_tem_pintura_curto_e_legacy(self):
        cats = server._CATEGORIAS_POR_UNIDADE.get("barueri", {})
        assert "CAUTELAR + PINTURA" in cats
        assert "CAUTELAR COM ANALISE DE PINTURA" in cats
        # Mesmo ID
        assert cats["CAUTELAR + PINTURA"] == cats["CAUTELAR COM ANALISE DE PINTURA"]

    def test_mooca_tem_pintura_curto_e_legacy(self):
        cats = server._CATEGORIAS_POR_UNIDADE.get("mooca", {})
        assert "CAUTELAR + PINTURA" in cats
        assert "CAUTELAR COM ANALISE DE PINTURA" in cats
        assert cats["CAUTELAR + PINTURA"] == cats["CAUTELAR COM ANALISE DE PINTURA"]
