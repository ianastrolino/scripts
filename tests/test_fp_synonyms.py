"""
Testes do lookup_config_id com sinonimos FA<->faturado e AV<->avista.

Bug confirmado em prod (Moema 04/05/2026): fp do PDV vinha como "faturado",
mapeamento da unidade tinha {"FA": 802165201, ...}, lookup falhava, e o Tiny
caia no default da empresa = Boleto.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tiny_import import lookup_config_id, _FP_SYNONYMS  # noqa: E402


# Mapeamento estilo Moema: chave "FA" (legacy de planilha)
MOEMA_FP_IDS = {
    "FA":       802165201,  # A faturar no Tiny
    "dinheiro": 556498207,
    "debito":   556498211,
    "credito":  556498209,
    "pix":      556498217,
    "detran":   556498213,
}


class TestSinonimosFaturado:
    def test_lookup_FA_legacy_continua_funcionando(self):
        """Planilha Sispevi exporta fp='FA' — deve achar."""
        assert lookup_config_id(MOEMA_FP_IDS, "FA") == 802165201

    def test_lookup_faturado_PDV_acha_via_sinonimo_FA(self):
        """Bug fix: PDV grava fp='faturado'; mapeamento tem 'FA'."""
        assert lookup_config_id(MOEMA_FP_IDS, "faturado") == 802165201

    def test_lookup_inverso_mapeamento_com_faturado_chave_FA_busca(self):
        """Mapeamento tem 'faturado', busca por 'FA' tambem deve achar."""
        m = {"faturado": 999}
        assert lookup_config_id(m, "FA") == 999

    def test_lookup_caso_normal_dinheiro_inalterado(self):
        assert lookup_config_id(MOEMA_FP_IDS, "dinheiro") == 556498207


class TestSinonimosAvista:
    def test_AV_acha_avista(self):
        m = {"avista": 100}
        assert lookup_config_id(m, "AV") == 100

    def test_avista_acha_AV(self):
        m = {"AV": 100}
        assert lookup_config_id(m, "avista") == 100


class TestNaoEncontrado:
    def test_chave_inexistente_retorna_none(self):
        assert lookup_config_id(MOEMA_FP_IDS, "carteirinha") is None

    def test_string_vazia_retorna_none(self):
        assert lookup_config_id(MOEMA_FP_IDS, "") is None

    def test_mapeamento_vazio_retorna_none(self):
        assert lookup_config_id({}, "faturado") is None


class TestCaseInsensitive:
    def test_caso_misto_acha(self):
        # normalize_key faz lowercase + strip
        assert lookup_config_id(MOEMA_FP_IDS, "Faturado") == 802165201
        assert lookup_config_id(MOEMA_FP_IDS, "FATURADO") == 802165201

    def test_pix_lowercase_em_chave_uppercase(self):
        m = {"PIX": 999}
        assert lookup_config_id(m, "pix") == 999


class TestSinonimoDictionary:
    def test_synonyms_bidirecional(self):
        """Documenta que _FP_SYNONYMS eh bidirecional."""
        assert _FP_SYNONYMS["fa"] == "faturado"
        assert _FP_SYNONYMS["faturado"] == "fa"
        assert _FP_SYNONYMS["av"] == "avista"
        assert _FP_SYNONYMS["avista"] == "av"
