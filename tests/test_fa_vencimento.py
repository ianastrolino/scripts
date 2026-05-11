"""
Testes do bug Tiny 06/05/2026:

Faturados estavam vindo com vencimento = data do servico (06/05) em vez do
ultimo dia do mes (31/05). Causa: wizard de fechamento marcava o registro
como "faturado" tambem no campo av_pagamento, e is_av_paid("faturado")
retornava True, fazendo o sistema tratar como AV pago.

Fix: is_av_paid retorna False pra 'faturado'/'fa' explicitamente.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tiny_import import (  # noqa: E402
    NormalizedRecord,
    TinyImporter,
    is_av_paid,
    last_day_of_month,
)


# ══════════════════════════════════════════════════════════════════════════════
# is_av_paid — caso por caso
# ══════════════════════════════════════════════════════════════════════════════

class TestIsAvPaid:
    def test_vazio_eh_false(self):
        assert is_av_paid("") is False

    def test_pendente_eh_false(self):
        assert is_av_paid("pendente") is False
        assert is_av_paid("PENDENTE") is False
        assert is_av_paid("pending") is False

    def test_faturado_eh_false(self):
        """Bug fix 06/05/2026: 'faturado' nao eh AV pago, eh FATURADO."""
        assert is_av_paid("faturado") is False
        assert is_av_paid("FATURADO") is False

    def test_fa_eh_false(self):
        """Variante curta tambem nao eh AV pago."""
        assert is_av_paid("fa") is False
        assert is_av_paid("FA") is False

    def test_dinheiro_eh_true(self):
        assert is_av_paid("dinheiro") is True

    def test_pix_eh_true(self):
        assert is_av_paid("pix") is True

    def test_debito_credito_sao_true(self):
        assert is_av_paid("debito") is True
        assert is_av_paid("credito") is True


# ══════════════════════════════════════════════════════════════════════════════
# Vencimento no payload — caminho completo
# ══════════════════════════════════════════════════════════════════════════════

def _make_rec(av_pagamento: str, fp: str = "FA", data: str = "2026-05-06") -> NormalizedRecord:
    return NormalizedRecord(
        data=data, modelo="GOL", placa="ABC1234", cliente="X",
        servico="VISTORIA CAUTELAR", fp=fp, preco="100.00",
        origem_arquivo="t.xls", linha_origem=1,
        chave_deduplicacao="x", av_pagamento=av_pagamento, cpf="",
    )


def _make_config() -> dict:
    return {
        "tiny": {
            "base_url": "https://api.tiny.com.br/public-api/v3",
            "token_url": "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token",
            "oauth_scope": "openid",
            "timeout_seconds": 30,
            "client_id": "test", "client_secret": "test",
            "redirect_uri": "http://localhost/cb",
            "scope": "openid",
            "cliente_ids": {},
            "forma_recebimento_ids": {"FA": 200, "dinheiro": 300, "pix": 400, "credito": 500, "debito": 600},
            "categoria_ids": {},
            "auto_create_contacts": False,
            "include_forma_recebimento": True,
            "numero_documento_prefix": "PLANILHA",
            "default_tipo_pessoa": "J",
            "require_payment_mapping": False,
            "vencimento_tipo": "ultimo_dia_mes",
            "vencimento_dias": 0,
            "contas_receber_fp": ["FA"],
            "servico_aliases": {},
            "fp_aliases": {},
            "cliente_aliases": {},
        }
    }


class TestVencimentoBugFix:
    def test_faturado_com_av_pagamento_faturado_vence_ultimo_dia_mes(self, tmp_path):
        """Bug do dia 06/05/2026: wizard marcava avPagamento='faturado',
        sistema tratava como AV pago e usava data do servico no vencimento.
        Fix: agora is_av_paid('faturado') = False → vencimento volta a ser
        ultimo dia do mes."""
        rec = _make_rec(av_pagamento="faturado", data="2026-05-06")
        importer = TinyImporter(_make_config(), tmp_path)
        # Mock resolve_contact e resolve_payment pra nao bater na rede
        with patch.object(importer, "resolve_contact", return_value=999), \
             patch.object(importer, "resolve_payment", return_value=200):
            payload = importer.build_accounts_receivable_payload(rec)
        # Data do servico: 06/05/2026 → ultimo dia do mes: 31/05/2026
        assert payload["dataVencimento"] == "2026-05-31"
        assert payload["data"] == "2026-05-06"  # data do servico inalterada

    def test_faturado_com_av_pagamento_vazio_tambem_vence_fim_de_mes(self, tmp_path):
        """Caso normal: planilha sem avPagamento, fp=FA → vence fim do mes."""
        rec = _make_rec(av_pagamento="", data="2026-05-06")
        importer = TinyImporter(_make_config(), tmp_path)
        with patch.object(importer, "resolve_contact", return_value=999), \
             patch.object(importer, "resolve_payment", return_value=200):
            payload = importer.build_accounts_receivable_payload(rec)
        assert payload["dataVencimento"] == "2026-05-31"

    def test_av_pix_vence_no_dia_do_servico(self, tmp_path):
        """Sanity check: AV pago com PIX continua vencendo no dia (regra
        antiga continua valendo)."""
        rec = _make_rec(av_pagamento="pix", fp="AV", data="2026-05-06")
        importer = TinyImporter(_make_config(), tmp_path)
        with patch.object(importer, "resolve_contact", return_value=999), \
             patch.object(importer, "resolve_payment", return_value=400):
            payload = importer.build_accounts_receivable_payload(rec)
        assert payload["dataVencimento"] == "2026-05-06"

    def test_av_dinheiro_vence_no_dia(self, tmp_path):
        rec = _make_rec(av_pagamento="dinheiro", fp="AV", data="2026-05-15")
        importer = TinyImporter(_make_config(), tmp_path)
        with patch.object(importer, "resolve_contact", return_value=999), \
             patch.object(importer, "resolve_payment", return_value=300):
            payload = importer.build_accounts_receivable_payload(rec)
        assert payload["dataVencimento"] == "2026-05-15"

    def test_dezembro_calcula_31_dezembro(self, tmp_path):
        """Edge case: ultimo mes do ano (dezembro)."""
        rec = _make_rec(av_pagamento="", data="2026-12-15")
        importer = TinyImporter(_make_config(), tmp_path)
        with patch.object(importer, "resolve_contact", return_value=999), \
             patch.object(importer, "resolve_payment", return_value=200):
            payload = importer.build_accounts_receivable_payload(rec)
        assert payload["dataVencimento"] == "2026-12-31"

    def test_fevereiro_ano_nao_bissexto(self, tmp_path):
        """Edge case: fevereiro 2026 tem 28 dias."""
        rec = _make_rec(av_pagamento="", data="2026-02-10")
        importer = TinyImporter(_make_config(), tmp_path)
        with patch.object(importer, "resolve_contact", return_value=999), \
             patch.object(importer, "resolve_payment", return_value=200):
            payload = importer.build_accounts_receivable_payload(rec)
        assert payload["dataVencimento"] == "2026-02-28"
