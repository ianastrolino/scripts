#!/usr/bin/env python3
"""
Testes unitários para tiny_import.py.
Execute com:  python -m pytest test_tiny_import.py -v
         ou:  python test_tiny_import.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch, call

# Garante que o módulo seja encontrado
sys.path.insert(0, str(Path(__file__).resolve().parent))

from tiny_import import (
    DEFAULT_CONFIG,
    NormalizedRecord,
    TinyClient,
    TinyImporter,
    build_history,
    clean_text,
    compact_document_number,
    due_date_for_record,
    last_day_of_month,
    lookup_config_id,
    merge_config,
    money_as_float,
    normalize_key,
    normalize_plate,
    parse_date,
    parse_money,
    record_key,
    remove_accents,
    similarity_score,
    should_send_accounts_receivable,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_record(**overrides) -> NormalizedRecord:
    defaults = dict(
        data="2026-04-13",
        modelo="I/PORSCHE 911",
        placa="FBA1I22",
        cliente="MARIN IMPORT",
        servico="LAUDO DE VERIFICACAO",
        fp="FA",
        preco="200.00",
        origem_arquivo="planilha.xls",
        linha_origem=2,
        chave_deduplicacao="abc123",
        av_pagamento="",
    )
    defaults.update(overrides)
    return NormalizedRecord(**defaults)


def _make_tiny_config(**overrides) -> dict:
    cfg = {
        "auth_url": "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/auth",
        "base_url": "https://api.tiny.com.br/public-api/v3",
        "token_url": "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token",
        "oauth_scope": "openid",
        "timeout_seconds": 30,
        "auto_create_contacts": False,
        "require_payment_mapping": False,
        "include_forma_recebimento": True,
        "default_tipo_pessoa": "J",
        "categoria_id": None,
        "contas_receber_fp": ["FA"],
        "vencimento_tipo": "ultimo_dia_mes",
        "vencimento_dias": 0,
        "numero_documento_prefix": "PLANILHA",
        "aliases": {"servico": {}, "fp": {}, "cliente": {}},
        "cliente_ids": {},
        "forma_recebimento_ids": {},
    }
    cfg.update(overrides)
    return cfg


# ---------------------------------------------------------------------------
# Testes de utilitários de texto
# ---------------------------------------------------------------------------

class TestCleanText(unittest.TestCase):
    def test_espaco_nbsp(self):
        self.assertEqual(clean_text("foo\xa0bar"), "foo bar")

    def test_espacos_multiplos(self):
        self.assertEqual(clean_text("  a   b  "), "a b")

    def test_vazio(self):
        self.assertEqual(clean_text(""), "")


class TestRemoveAccents(unittest.TestCase):
    def test_acento(self):
        self.assertEqual(remove_accents("Aviação"), "Aviacao")

    def test_cedilha(self):
        self.assertEqual(remove_accents("Verificação"), "Verificacao")


class TestNormalizeKey(unittest.TestCase):
    def test_caixa_alta_sem_acento(self):
        self.assertEqual(normalize_key("Marin Import"), "marin import")

    def test_caracteres_especiais(self):
        self.assertEqual(normalize_key("ABC-123!@#"), "abc 123")


class TestNormalizePlate(unittest.TestCase):
    def test_placa_mercosul(self):
        self.assertEqual(normalize_plate("FBA-1I22"), "FBA1I22")

    def test_placa_antiga(self):
        self.assertEqual(normalize_plate("abc 1234"), "ABC1234")


# ---------------------------------------------------------------------------
# Testes de parse de data e dinheiro
# ---------------------------------------------------------------------------

class TestParseDate(unittest.TestCase):
    def test_formato_br(self):
        self.assertEqual(parse_date("13/04/2026"), "2026-04-13")

    def test_formato_iso(self):
        self.assertEqual(parse_date("2026-04-13"), "2026-04-13")

    def test_invalido(self):
        with self.assertRaises(ValueError):
            parse_date("nao-e-data")


class TestParseMoney(unittest.TestCase):
    def test_formato_br(self):
        self.assertEqual(parse_money("R$ 1.500,50"), Decimal("1500.50"))

    def test_formato_us(self):
        self.assertEqual(parse_money("1500.50"), Decimal("1500.50"))

    def test_inteiro(self):
        self.assertEqual(parse_money("200"), Decimal("200.00"))

    def test_vazio(self):
        with self.assertRaises(ValueError):
            parse_money("")


class TestMoneyAsFloat(unittest.TestCase):
    def test_basico(self):
        self.assertAlmostEqual(money_as_float("200.00"), 200.0)


# ---------------------------------------------------------------------------
# Testes de datas de vencimento
# ---------------------------------------------------------------------------

class TestLastDayOfMonth(unittest.TestCase):
    def test_abril(self):
        self.assertEqual(last_day_of_month("2026-04-13"), "2026-04-30")

    def test_fevereiro_bissexto(self):
        self.assertEqual(last_day_of_month("2024-02-01"), "2024-02-29")

    def test_fevereiro_nao_bissexto(self):
        self.assertEqual(last_day_of_month("2026-02-15"), "2026-02-28")


class TestDueDateForRecord(unittest.TestCase):
    def test_ultimo_dia_mes(self):
        record = _make_record(data="2026-04-13")
        cfg = _make_tiny_config(vencimento_tipo="ultimo_dia_mes")
        self.assertEqual(due_date_for_record(record, cfg), "2026-04-30")

    def test_mesma_data(self):
        record = _make_record(data="2026-04-13")
        cfg = _make_tiny_config(vencimento_tipo="mesma data")
        self.assertEqual(due_date_for_record(record, cfg), "2026-04-13")

    def test_dias(self):
        record = _make_record(data="2026-04-13")
        cfg = _make_tiny_config(vencimento_tipo="dias", vencimento_dias=10)
        self.assertEqual(due_date_for_record(record, cfg), "2026-04-23")

    def test_av_usa_data_servico(self):
        """Registros AV já recebidos: vencimento = data do serviço."""
        record = _make_record(data="2026-04-13", av_pagamento="dinheiro")
        cfg = _make_tiny_config(vencimento_tipo="ultimo_dia_mes")
        # build_accounts_receivable_payload usa record.data quando av_pagamento está preenchido
        due = record.data if record.av_pagamento else due_date_for_record(record, cfg)
        self.assertEqual(due, "2026-04-13")


# ---------------------------------------------------------------------------
# Testes de lookup e similarity
# ---------------------------------------------------------------------------

class TestLookupConfigId(unittest.TestCase):
    def setUp(self):
        self.mapping = {
            "MARIN IMPORT": 566890464,
            "Europamotors": 123456789,
            "dinheiro": 556498207,
        }

    def test_correspondencia_exata(self):
        self.assertEqual(lookup_config_id(self.mapping, "MARIN IMPORT"), 566890464)

    def test_correspondencia_normalizada(self):
        self.assertEqual(lookup_config_id(self.mapping, "marin import"), 566890464)
        self.assertEqual(lookup_config_id(self.mapping, "EUROPAMOTORS"), 123456789)

    def test_nao_encontrado(self):
        self.assertIsNone(lookup_config_id(self.mapping, "INEXISTENTE"))

    def test_dinheiro_lowercase(self):
        self.assertEqual(lookup_config_id(self.mapping, "dinheiro"), 556498207)

    def test_dinheiro_uppercase(self):
        """Chave 'dinheiro' no config deve bater com 'DINHEIRO' (normalizado)."""
        self.assertEqual(lookup_config_id(self.mapping, "DINHEIRO"), 556498207)


class TestSimilarityScore(unittest.TestCase):
    def test_iguais(self):
        self.assertEqual(similarity_score("Marin Import", "Marin Import"), 1.0)

    def test_contem(self):
        score = similarity_score("Marin", "Marin Import")
        self.assertGreater(score, 0.5)

    def test_sem_semelhanca(self):
        score = similarity_score("ABC", "XYZ")
        self.assertEqual(score, 0.0)

    def test_vazio(self):
        self.assertEqual(similarity_score("", "qualquer"), 0.0)


# ---------------------------------------------------------------------------
# Testes de chave de deduplicação e número de documento
# ---------------------------------------------------------------------------

class TestRecordKey(unittest.TestCase):
    def test_deterministico(self):
        r = dict(
            data="2026-04-13", modelo="PORSCHE 911", placa="FBA1I22",
            cliente="MARIN IMPORT", servico="LAUDO DE VERIFICACAO", preco="200.00",
        )
        self.assertEqual(record_key(r), record_key(r))

    def test_diferente_por_placa(self):
        r1 = dict(data="2026-04-13", modelo="X", placa="AAA1111",
                  cliente="CLI", servico="SVC", preco="100.00")
        r2 = dict(data="2026-04-13", modelo="X", placa="BBB2222",
                  cliente="CLI", servico="SVC", preco="100.00")
        self.assertNotEqual(record_key(r1), record_key(r2))


class TestCompactDocumentNumber(unittest.TestCase):
    def test_formato_9_chars(self):
        cfg = _make_tiny_config()
        rec = _make_record(data="2026-04-15", linha_origem=2)
        resultado = compact_document_number(cfg, rec)
        self.assertEqual(len(resultado), 9, "Tiny exige exatamente 9 caracteres no formato faturar")

    def test_linha_grande_truncada_em_3_digitos(self):
        cfg = _make_tiny_config()
        rec = _make_record(data="2026-04-15", linha_origem=1005)
        resultado = compact_document_number(cfg, rec)
        self.assertEqual(len(resultado), 9)

    def test_sempre_9_chars(self):
        cfg = _make_tiny_config()
        for linha in [1, 10, 99, 100, 999]:
            rec = _make_record(data="2026-04-15", linha_origem=linha)
            resultado = compact_document_number(cfg, rec)
            self.assertEqual(len(resultado), 9, f"linha={linha}: esperado 9 chars, got '{resultado}'")


# ---------------------------------------------------------------------------
# Testes de histórico
# ---------------------------------------------------------------------------

class TestBuildHistory(unittest.TestCase):
    def test_contem_campos_principais(self):
        rec = _make_record()
        h = build_history(rec)
        self.assertIn("FBA1I22", h)
        self.assertIn("I/PORSCHE 911", h)

    def test_max_250_chars(self):
        rec = _make_record(servico="S" * 300)
        self.assertLessEqual(len(build_history(rec)), 250)


# ---------------------------------------------------------------------------
# Testes de should_send_accounts_receivable
# ---------------------------------------------------------------------------

class TestShouldSendAccountsReceivable(unittest.TestCase):
    def test_fa_enviavel(self):
        rec = _make_record(fp="FA")
        cfg = _make_tiny_config(contas_receber_fp=["FA"])
        self.assertTrue(should_send_accounts_receivable(rec, cfg))

    def test_av_nao_enviavel_por_padrao(self):
        rec = _make_record(fp="AV")
        cfg = _make_tiny_config(contas_receber_fp=["FA"])
        self.assertFalse(should_send_accounts_receivable(rec, cfg))

    def test_lista_vazia_nao_envia(self):
        rec = _make_record(fp="FA")
        cfg = _make_tiny_config(contas_receber_fp=[])
        self.assertFalse(should_send_accounts_receivable(rec, cfg))


# ---------------------------------------------------------------------------
# Testes do TinyImporter.build_accounts_receivable_payload
# ---------------------------------------------------------------------------

class TestBuildAccountsReceivablePayload(unittest.TestCase):
    """Testa a construção do payload enviado ao Tiny — especialmente o formato de formaRecebimento."""

    def _make_importer(self, tiny_cfg: dict, state_dir: Path) -> TinyImporter:
        full_config = merge_config(DEFAULT_CONFIG, {"tiny": tiny_cfg})
        return TinyImporter(full_config, state_dir)

    def test_forma_recebimento_e_inteiro(self):
        """formaRecebimento deve ser int direto (Tiny nao aceita objeto stdClass aqui)."""
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            tiny_cfg = _make_tiny_config(
                cliente_ids={"MARIN IMPORT": 566890464},
                forma_recebimento_ids={"FA": 802165201},
                include_forma_recebimento=True,
            )
            importer = self._make_importer(tiny_cfg, state_dir)
            importer.client = MagicMock()
            rec = _make_record(fp="FA", av_pagamento="")
            payload = importer.build_accounts_receivable_payload(rec)

            self.assertIn("formaRecebimento", payload)
            self.assertIsInstance(payload["formaRecebimento"], int,
                                   "formaRecebimento deve ser int, nao dict!")
            self.assertEqual(payload["formaRecebimento"], 802165201)

    def test_contato_usa_id_configurado(self):
        """contato deve ser {"id": <id do config>}, não {"nome": ...}."""
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            tiny_cfg = _make_tiny_config(
                cliente_ids={"MARIN IMPORT": 566890464},
                forma_recebimento_ids={},
                include_forma_recebimento=False,
            )
            importer = self._make_importer(tiny_cfg, state_dir)
            importer.client = MagicMock()
            rec = _make_record(fp="FA")
            payload = importer.build_accounts_receivable_payload(rec)

            self.assertIn("contato", payload)
            self.assertIn("id", payload["contato"],
                          "contato deve ter campo 'id'")
            self.assertEqual(payload["contato"]["id"], 566890464)

    def test_av_usa_data_servico_como_vencimento(self):
        """Registros AV (à vista) devem usar a data do serviço como vencimento."""
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            tiny_cfg = _make_tiny_config(
                cliente_ids={"PARTICULAR MOEMA": 999},
                forma_recebimento_ids={"dinheiro": 556498207},
                include_forma_recebimento=True,
                vencimento_tipo="ultimo_dia_mes",
            )
            importer = self._make_importer(tiny_cfg, state_dir)
            importer.client = MagicMock()
            rec = _make_record(
                data="2026-04-13",
                cliente="PARTICULAR MOEMA",
                fp="AV",
                av_pagamento="dinheiro",
            )
            payload = importer.build_accounts_receivable_payload(rec)

            self.assertEqual(payload["data"], "2026-04-13")
            self.assertEqual(payload["dataVencimento"], "2026-04-13",
                             "AV: vencimento deve ser a mesma data do serviço")

    def test_av_pagamento_mapeado_como_forma_recebimento(self):
        """Para AV, formaRecebimento é o ID inteiro do pagamento (dinheiro/debito/credito/pix)."""
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            tiny_cfg = _make_tiny_config(
                cliente_ids={"PARTICULAR MOEMA": 999},
                forma_recebimento_ids={"dinheiro": 556498207, "debito": 556498211},
                include_forma_recebimento=True,
            )
            importer = self._make_importer(tiny_cfg, state_dir)
            importer.client = MagicMock()

            for fp_key, expected_id in [("dinheiro", 556498207), ("debito", 556498211)]:
                rec = _make_record(
                    cliente="PARTICULAR MOEMA",
                    fp="AV",
                    av_pagamento=fp_key,
                )
                payload = importer.build_accounts_receivable_payload(rec)
                self.assertIsInstance(payload["formaRecebimento"], int,
                                      f"formaRecebimento deve ser int para '{fp_key}'")
                self.assertEqual(payload["formaRecebimento"], expected_id,
                                 f"Mapeamento falhou para '{fp_key}'")

    def test_sem_forma_recebimento_quando_nao_mapeado(self):
        """Quando a forma de pagamento não está mapeada e require_payment_mapping=False,
        o payload não deve incluir formaRecebimento (não deve quebrar)."""
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            tiny_cfg = _make_tiny_config(
                cliente_ids={"MARIN IMPORT": 566890464},
                forma_recebimento_ids={},  # Vazio — sem mapeamento
                include_forma_recebimento=True,
                require_payment_mapping=False,
            )
            importer = self._make_importer(tiny_cfg, state_dir)
            importer.client = MagicMock()
            rec = _make_record(fp="FA", av_pagamento="")
            payload = importer.build_accounts_receivable_payload(rec)

            # Sem mapeamento: formaRecebimento não deve estar no payload
            self.assertNotIn("formaRecebimento", payload)

    def test_categoria_id_no_payload(self):
        """categoria_id deve aparecer como {"id": int} no payload."""
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            tiny_cfg = _make_tiny_config(
                cliente_ids={"MARIN IMPORT": 566890464},
                categoria_id=777,
            )
            importer = self._make_importer(tiny_cfg, state_dir)
            importer.client = MagicMock()
            rec = _make_record(fp="FA")
            payload = importer.build_accounts_receivable_payload(rec)

            self.assertIn("categoria", payload)
            self.assertEqual(payload["categoria"]["id"], 777)

    def test_campos_obrigatorios_presentes(self):
        """Payload deve ter todos os campos obrigatórios da API Tiny."""
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            tiny_cfg = _make_tiny_config(cliente_ids={"MARIN IMPORT": 566890464})
            importer = self._make_importer(tiny_cfg, state_dir)
            importer.client = MagicMock()
            rec = _make_record()
            payload = importer.build_accounts_receivable_payload(rec)

            campos_obrigatorios = [
                "data", "dataVencimento", "dataCompetencia",
                "valor", "contato", "numeroDocumento", "historico", "ocorrencia",
            ]
            for campo in campos_obrigatorios:
                self.assertIn(campo, payload, f"Campo obrigatório ausente: {campo}")

    def test_ocorrencia_unica(self):
        """ocorrencia deve ser 'U' (única)."""
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            tiny_cfg = _make_tiny_config(cliente_ids={"MARIN IMPORT": 566890464})
            importer = self._make_importer(tiny_cfg, state_dir)
            importer.client = MagicMock()
            rec = _make_record()
            payload = importer.build_accounts_receivable_payload(rec)
            self.assertEqual(payload["ocorrencia"], "U")


# ---------------------------------------------------------------------------
# Testes de TinyClient (sem rede)
# ---------------------------------------------------------------------------

class TestTinyClientTokenRefresh(unittest.TestCase):
    def test_salva_refresh_token_no_arquivo(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            tiny_cfg = _make_tiny_config(
                client_id="cid",
                client_secret="csec",
                refresh_token="rt_inicial",
            )
            client = TinyClient(tiny_cfg, state_dir)

            fake_response = MagicMock()
            fake_response.status_code = 200
            fake_response.json.return_value = {
                "access_token": "at_novo",
                "refresh_token": "rt_novo",
                "expires_in": 14400,
            }

            with patch("requests.post", return_value=fake_response):
                token = client.refresh_access_token()

            self.assertEqual(token, "at_novo")
            saved = json.loads((state_dir / "tiny_tokens.json").read_text())
            self.assertEqual(saved["access_token"], "at_novo")
            self.assertEqual(saved["refresh_token"], "rt_novo")

    def test_forma_recebimento_formato_correto_no_request(self):
        """Verifica que o payload enviado ao Tiny tem formaRecebimento como objeto."""
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            tiny_cfg = _make_tiny_config(
                cliente_ids={"MARIN IMPORT": 566890464},
                forma_recebimento_ids={"FA": 802165201},
                include_forma_recebimento=True,
            )
            full_cfg = merge_config(DEFAULT_CONFIG, {"tiny": tiny_cfg})
            importer = TinyImporter(full_cfg, state_dir)

            captured_payloads = []

            def fake_request(method, path, *, params=None, json_body=None, retry_auth=True):
                if method == "POST":
                    captured_payloads.append(json_body)
                return {"id": 99}

            importer.client.request = fake_request
            rec = _make_record(fp="FA")
            importer.create_accounts_receivable(rec)

            self.assertEqual(len(captured_payloads), 1)
            payload = captured_payloads[0]
            self.assertIn("formaRecebimento", payload)
            self.assertIsInstance(payload["formaRecebimento"], int)
            self.assertEqual(payload["formaRecebimento"], 802165201)


# ---------------------------------------------------------------------------
# Testes de merge_config
# ---------------------------------------------------------------------------

class TestMergeConfig(unittest.TestCase):
    def test_merge_simples(self):
        base = {"a": 1, "b": {"c": 2, "d": 3}}
        override = {"b": {"c": 99}}
        result = merge_config(base, override)
        self.assertEqual(result["a"], 1)
        self.assertEqual(result["b"]["c"], 99)
        self.assertEqual(result["b"]["d"], 3)  # Mantém valores não sobrepostos

    def test_sobrescreve_primitivo(self):
        base = {"x": "antigo"}
        override = {"x": "novo"}
        self.assertEqual(merge_config(base, override)["x"], "novo")

    def test_nao_modifica_original(self):
        base = {"a": {"b": 1}}
        merge_config(base, {"a": {"c": 2}})
        self.assertNotIn("c", base["a"])  # Base não alterada


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(verbosity=2)
