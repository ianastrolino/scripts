"""
Testes do TinyImporter — caminho do dinheiro entrando no Tiny.

Estrategia: mock TinyClient.request (unico ponto de saida pra rede).
Cobre: build_payload, resolve_contact, resolve_payment, idempotencia,
auto_create_contacts, require_payment_mapping, vencimento AV/FA.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tiny_import import (  # noqa: E402
    NormalizedRecord,
    TinyApiError,
    TinyImporter,
)


def _make_record(**overrides) -> NormalizedRecord:
    base = dict(
        data="2026-04-26",
        modelo="GOL",
        placa="ABC1234",
        cliente="CLIENTE TESTE",
        servico="LAUDO DE TRANSFERENCIA",
        fp="AV",
        preco="150.00",
        origem_arquivo="planilha.xls",
        linha_origem=2,
        chave_deduplicacao="2026-04-26|ABC1234|LAUDO|150.00",
        av_pagamento="pix",
        cpf="",
    )
    base.update(overrides)
    return NormalizedRecord(**base)


def _make_config(**overrides) -> dict:
    base = {
        "tiny": {
            "base_url": "https://api.tiny.com.br/public-api/v3",
            "token_url": "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token",
            "oauth_scope": "openid",
            "timeout_seconds": 30,
            "client_id": "test", "client_secret": "test",
            "redirect_uri": "http://localhost/cb",
            "scope": "openid",
            "cliente_ids": {},
            "forma_recebimento_ids": {"AV": 100, "FA": 200, "dinheiro": 300, "pix": 400, "credito": 500, "debito": 600},
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
    if overrides:
        base["tiny"].update(overrides)
    return base


@pytest.fixture
def importer(tmp_path):
    """TinyImporter com TinyClient.request mockado — sem rede."""
    config = _make_config()
    imp = TinyImporter(config, tmp_path)
    imp.client.request = MagicMock()
    return imp


# ══════════════════════════════════════════════════════════════════════════════
# resolve_contact
# ══════════════════════════════════════════════════════════════════════════════

class TestResolveContact:
    def test_usa_cliente_ids_mapeados_sem_chamar_api(self, importer):
        importer.config["cliente_ids"] = {"CLIENTE TESTE": 999}
        cid = importer.resolve_contact("CLIENTE TESTE")
        assert cid == 999
        importer.client.request.assert_not_called()

    def test_busca_por_nome_quando_nao_mapeado(self, importer):
        importer.client.request.return_value = {"itens": [{"id": 555, "nome": "CLIENTE TESTE"}]}
        cid = importer.resolve_contact("CLIENTE TESTE")
        assert cid == 555
        importer.client.request.assert_called_with("GET", "contatos", params={"nome": "CLIENTE TESTE", "limit": 100})

    def test_busca_por_cpf_filtra_local(self, importer):
        # API Tiny v3 nao respeita cpf_cnpj — precisa filtrar local
        importer.client.request.return_value = {"itens": [
            {"id": 111, "nome": "OUTRO", "cpfCnpj": None},
            {"id": 222, "nome": "CLIENTE TESTE", "cpfCnpj": "12345678901"},
        ]}
        cid = importer.resolve_contact("CLIENTE TESTE", cpf="123.456.789-01")
        assert cid == 222

    def test_nao_encontrado_sem_auto_create_levanta_erro(self, importer):
        importer.client.request.return_value = {"itens": []}
        with pytest.raises(TinyApiError, match="Cliente nao encontrado"):
            importer.resolve_contact("DESCONHECIDO")

    def test_nao_encontrado_com_auto_create_cria_contato(self, importer):
        importer.config["auto_create_contacts"] = True
        importer.client.request.side_effect = [
            {"itens": []},        # busca por nome — vazio
            {"id": 777, "nome": "NOVO"},  # POST cria
        ]
        cid = importer.resolve_contact("NOVO")
        assert cid == 777
        # 2a chamada eh POST com body
        post_call = importer.client.request.call_args_list[1]
        assert post_call.args[0] == "POST"
        assert post_call.args[1] == "contatos"
        assert post_call.kwargs["json_body"]["nome"] == "NOVO"

    def test_cache_evita_2a_chamada_pra_mesmo_cliente(self, importer):
        importer.client.request.return_value = {"itens": [{"id": 333, "nome": "CACHED"}]}
        importer.resolve_contact("CACHED")
        importer.resolve_contact("CACHED")
        assert importer.client.request.call_count == 1


# ══════════════════════════════════════════════════════════════════════════════
# resolve_payment
# ══════════════════════════════════════════════════════════════════════════════

class TestResolvePayment:
    def test_mapeado_no_config(self, importer):
        # config padrao tem AV→100, FA→200, etc
        assert importer.resolve_payment("AV") == 100
        assert importer.resolve_payment("FA") == 200
        assert importer.resolve_payment("pix") == 400

    def test_fp_vazio_retorna_none(self, importer):
        assert importer.resolve_payment("") is None

    def test_busca_no_tiny_quando_nao_mapeado(self, importer):
        importer.client.request.return_value = {"itens": [{"id": 888, "nome": "NOVA_FP"}]}
        pid = importer.resolve_payment("NOVA_FP")
        assert pid == 888

    def test_nao_encontrado_sem_require_retorna_none(self, importer):
        importer.client.request.return_value = {"itens": []}
        assert importer.resolve_payment("INEXISTENTE") is None

    def test_nao_encontrado_com_require_levanta(self, importer):
        importer.config["require_payment_mapping"] = True
        importer.client.request.return_value = {"itens": []}
        with pytest.raises(TinyApiError, match="Forma de recebimento"):
            importer.resolve_payment("INEXISTENTE")


# ══════════════════════════════════════════════════════════════════════════════
# build_accounts_receivable_payload
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildPayload:
    def test_av_pago_vencimento_eh_data_do_servico(self, importer):
        importer.config["cliente_ids"] = {"CLIENTE TESTE": 1}
        rec = _make_record(av_pagamento="pix", data="2026-04-15")
        payload = importer.build_accounts_receivable_payload(rec)
        assert payload["data"] == "2026-04-15"
        assert payload["dataVencimento"] == "2026-04-15"  # AV recebido na hora
        assert payload["valor"] == 150.00
        assert payload["contato"]["id"] == 1

    def test_fa_vencimento_eh_ultimo_dia_do_mes(self, importer):
        importer.config["cliente_ids"] = {"CLIENTE TESTE": 1}
        rec = _make_record(fp="FA", av_pagamento="", data="2026-04-15")
        payload = importer.build_accounts_receivable_payload(rec)
        assert payload["dataVencimento"] == "2026-04-30"

    def test_inclui_forma_recebimento_quando_habilitado(self, importer):
        importer.config["cliente_ids"] = {"CLIENTE TESTE": 1}
        rec = _make_record(av_pagamento="pix")
        payload = importer.build_accounts_receivable_payload(rec)
        assert payload["formaRecebimento"] == 400  # pix → 400

    def test_omite_forma_recebimento_quando_desabilitado(self, importer):
        importer.config["cliente_ids"] = {"CLIENTE TESTE": 1}
        importer.config["include_forma_recebimento"] = False
        rec = _make_record(av_pagamento="pix")
        payload = importer.build_accounts_receivable_payload(rec)
        assert "formaRecebimento" not in payload

    def test_inclui_categoria_quando_mapeada(self, importer):
        importer.config["cliente_ids"] = {"CLIENTE TESTE": 1}
        importer.config["categoria_ids"] = {"LAUDO DE TRANSFERENCIA": 999}
        rec = _make_record(servico="LAUDO DE TRANSFERENCIA")
        payload = importer.build_accounts_receivable_payload(rec)
        assert payload["categoria"]["id"] == 999

    def test_omite_categoria_quando_servico_nao_mapeado(self, importer):
        importer.config["cliente_ids"] = {"CLIENTE TESTE": 1}
        rec = _make_record(servico="SERVICO_SEM_CATEGORIA")
        payload = importer.build_accounts_receivable_payload(rec)
        assert "categoria" not in payload

    def test_competencia_eh_yyyy_mm(self, importer):
        importer.config["cliente_ids"] = {"CLIENTE TESTE": 1}
        rec = _make_record(data="2026-04-15")
        payload = importer.build_accounts_receivable_payload(rec)
        assert payload["dataCompetencia"] == "2026-04"

    def test_ocorrencia_unica(self, importer):
        importer.config["cliente_ids"] = {"CLIENTE TESTE": 1}
        rec = _make_record()
        payload = importer.build_accounts_receivable_payload(rec)
        assert payload["ocorrencia"] == "U"


# ══════════════════════════════════════════════════════════════════════════════
# create_accounts_receivable + idempotencia
# ══════════════════════════════════════════════════════════════════════════════

class TestCreateAccountsReceivable:
    def test_chama_post_com_payload_montado(self, importer):
        importer.config["cliente_ids"] = {"CLIENTE TESTE": 1}
        importer.client.request.return_value = {"id": 12345, "numeroDocumento": "PLN-001"}
        rec = _make_record()
        result = importer.create_accounts_receivable(rec)
        assert result["id"] == 12345
        post_call = importer.client.request.call_args
        assert post_call.args == ("POST", "contas-receber")
        assert "json_body" in post_call.kwargs
        assert post_call.kwargs["json_body"]["valor"] == 150.00

    def test_check_existing_retorna_true_quando_encontra(self, importer):
        importer.client.request.return_value = {"itens": [{"id": 999}]}
        assert importer.check_existing_by_numero_documento("PLN-001") is True

    def test_check_existing_retorna_false_quando_vazio(self, importer):
        importer.client.request.return_value = {"itens": []}
        assert importer.check_existing_by_numero_documento("PLN-001") is False

    def test_check_existing_retorna_false_em_erro(self, importer):
        importer.client.request.side_effect = TinyApiError("rede caiu")
        assert importer.check_existing_by_numero_documento("PLN-001") is False
