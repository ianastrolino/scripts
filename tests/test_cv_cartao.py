"""
Testes do campo CV (Codigo de Verificacao) pra pagamentos no cartao.

Bug operacional 04/05/2026: ate hoje nao havia rastreio do CV no fechamento;
quando bate o extrato da maquininha, era impossivel identificar qual venda
corresponde a qual transacao do cartao. Agora:
- CV obrigatorio em fp=debito/credito
- CV salvo em lancamentos.cv (coluna nova com migration)
- CV vai pro historico Tiny: 'Placa XXX | MODELO | SERVICO | CV: 12345 | CPF...'
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
from tiny_import import NormalizedRecord, build_history  # noqa: E402

UNIT = "sp"
UNITS_FIX = {"sp": {"nome": "São Paulo", "master_pin": "0000"}}
USER = {"email": "op@astro.com", "name": "Op", "unit": "sp", "master": False}


@pytest.fixture
def client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=USER):
        with server.app.test_client() as c:
            yield c


# ══════════════════════════════════════════════════════════════════════════════
# build_history com CV
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildHistoryComCV:
    def _rec(self, **kw):
        defaults = dict(
            data="2026-05-04", modelo="FORD KA", placa="ABC1234",
            cliente="X", servico="VISTORIA CAUTELAR", fp="debito",
            preco="100", origem_arquivo="t", linha_origem=0,
            chave_deduplicacao="x", av_pagamento="debito", cpf="",
        )
        defaults.update(kw)
        return NormalizedRecord(**defaults)

    def test_cv_aparece_no_historico(self):
        rec = self._rec(cv="16618216")
        h = build_history(rec)
        assert "CV: 16618216" in h

    def test_cv_vazio_omite(self):
        rec = self._rec(cv="")
        h = build_history(rec)
        assert "CV:" not in h

    def test_ordem_placa_modelo_servico_cv(self):
        rec = self._rec(cv="999")
        h = build_history(rec)
        i_placa = h.index("Placa ABC1234")
        i_modelo = h.index("FORD KA")
        i_servico = h.index("VISTORIA CAUTELAR")
        i_cv = h.index("CV: 999")
        assert i_placa < i_modelo < i_servico < i_cv

    def test_cv_antes_do_cpf(self):
        rec = self._rec(cv="999", cpf="12345678901")
        h = build_history(rec)
        assert h.index("CV:") < h.index("CPF")


# ══════════════════════════════════════════════════════════════════════════════
# api_caixa_lancar com CV
# ══════════════════════════════════════════════════════════════════════════════

class TestLancarComCV:
    def _payload(self, **kw):
        base = {
            "placa": "ABC1234", "cliente": "TESTE",
            "servico": "VISTORIA CAUTELAR", "valor": 100.0, "fp": "debito",
            "cv": "12345678",
        }
        base.update(kw)
        return base

    def test_lancamento_debito_com_cv_ok(self, client):
        r = client.post(f"/u/{UNIT}/api/caixa/lancar", json=self._payload(fp="debito"))
        assert r.status_code == 200
        assert r.get_json()["lancamento"]["cv"] == "12345678"

    def test_lancamento_credito_com_cv_ok(self, client):
        r = client.post(f"/u/{UNIT}/api/caixa/lancar", json=self._payload(fp="credito", cv="999"))
        assert r.status_code == 200
        assert r.get_json()["lancamento"]["cv"] == "999"

    def test_lancamento_debito_sem_cv_400(self, client):
        r = client.post(f"/u/{UNIT}/api/caixa/lancar", json=self._payload(fp="debito", cv=""))
        assert r.status_code == 400
        assert "CV" in r.get_json()["error"]

    def test_lancamento_credito_sem_cv_400(self, client):
        r = client.post(f"/u/{UNIT}/api/caixa/lancar", json=self._payload(fp="credito", cv=""))
        assert r.status_code == 400

    def test_lancamento_pix_sem_cv_ok(self, client):
        """PIX nao precisa de CV (so debito/credito)."""
        r = client.post(f"/u/{UNIT}/api/caixa/lancar", json=self._payload(fp="pix", cv=""))
        assert r.status_code == 200

    def test_lancamento_dinheiro_sem_cv_ok(self, client):
        r = client.post(f"/u/{UNIT}/api/caixa/lancar", json=self._payload(fp="dinheiro", cv=""))
        assert r.status_code == 200

    def test_cv_salvo_no_caixa_estado(self, client):
        client.post(f"/u/{UNIT}/api/caixa/lancar", json=self._payload(fp="debito", cv="888"))
        body = client.get(f"/u/{UNIT}/api/caixa/estado").get_json()
        assert body["lancamentos"][0]["cv"] == "888"


# ══════════════════════════════════════════════════════════════════════════════
# api_caixa_editar com CV
# ══════════════════════════════════════════════════════════════════════════════

class TestEditarComCV:
    @pytest.fixture(autouse=True)
    def _seed_pin(self, tmp_path):
        # _save_pin_store usa _PINS_FILE = DATA_DIR/pins.json — tmp_path nao
        # afeta esse path. Pra teste, mock _verify_unit_pin.
        with patch.object(server, "_verify_unit_pin", return_value=True):
            yield

    def test_editar_pra_credito_exige_cv(self, client):
        r = client.post(f"/u/{UNIT}/api/caixa/lancar", json={
            "placa": "ABC1234", "cliente": "X",
            "servico": "VISTORIA CAUTELAR", "valor": 100.0, "fp": "pix",
        })
        lc_id = r.get_json()["lancamento"]["id"]
        # Editar pra credito sem CV → 400
        r2 = client.put(f"/u/{UNIT}/api/caixa/editar/{lc_id}", json={
            "pin": "1234", "placa": "ABC1234", "cliente": "X",
            "servico": "VISTORIA CAUTELAR", "valor": 100.0, "fp": "credito",
        })
        assert r2.status_code == 400
        # Mesma edicao com CV → 200
        r3 = client.put(f"/u/{UNIT}/api/caixa/editar/{lc_id}", json={
            "pin": "1234", "placa": "ABC1234", "cliente": "X",
            "servico": "VISTORIA CAUTELAR", "valor": 100.0, "fp": "credito",
            "cv": "999",
        })
        assert r3.status_code == 200


# ══════════════════════════════════════════════════════════════════════════════
# /api/caixa/conferir devolve CV do PDV
# ══════════════════════════════════════════════════════════════════════════════
# Bug 12/05/2026: lancamentos em cartao na planilha vinham sem CV no Tiny.
# Planilha nao tem CV — vinha do PDV. Match planilha×PDV precisa devolver o CV
# do lancamento do PDV pro frontend propagar pro record antes de enviar pro ERP.

class TestConferirDevolveCV:
    def test_match_ok_inclui_pdv_cv(self, client):
        # 1. Lanca debito no PDV com CV
        client.post(f"/u/{UNIT}/api/caixa/lancar", json={
            "placa": "XYZ9999", "cliente": "FULANO",
            "servico": "VISTORIA CAUTELAR", "valor": 100.0, "fp": "debito",
            "cv": "16618216",
        })
        # 2. Confere com record da planilha (mesmo placa/servico/preco)
        r = client.post(f"/u/{UNIT}/api/caixa/conferir", json={
            "records": [{
                "id": "rec-1", "placa": "XYZ9999",
                "servico": "VISTORIA CAUTELAR", "preco": 100.0, "fp": "AV",
            }],
        })
        assert r.status_code == 200
        conf = r.get_json()["conferencia"]["rec-1"]
        assert conf["status"] == "ok"
        assert conf["pdv_cv"] == "16618216"

    def test_match_ok_fallback_inclui_pdv_cv(self, client):
        # Servico no PDV difere da planilha → match por placa+valor (fallback)
        client.post(f"/u/{UNIT}/api/caixa/lancar", json={
            "placa": "XYZ8888", "cliente": "X",
            "servico": "VISTORIA CAUTELAR", "valor": 200.0, "fp": "credito",
            "cv": "55555",
        })
        r = client.post(f"/u/{UNIT}/api/caixa/conferir", json={
            "records": [{
                "id": "rec-2", "placa": "XYZ8888",
                "servico": "LAUDO DE TRANSFERENCIA",  # divergente
                "preco": 200.0, "fp": "AV",
            }],
        })
        conf = r.get_json()["conferencia"]["rec-2"]
        assert conf["status"] == "ok_fallback"
        assert conf["pdv_cv"] == "55555"

    def test_sem_pdv_pdv_cv_vazio(self, client):
        r = client.post(f"/u/{UNIT}/api/caixa/conferir", json={
            "records": [{
                "id": "rec-x", "placa": "AAA0000",
                "servico": "VISTORIA CAUTELAR", "preco": 100.0, "fp": "AV",
            }],
        })
        conf = r.get_json()["conferencia"]["rec-x"]
        assert conf["status"] == "sem_pdv"
        assert conf["pdv_cv"] == ""

    def test_pix_sem_cv_devolve_string_vazia(self, client):
        # Lancamento sem cv (PIX) — match deve devolver string vazia, nao None
        client.post(f"/u/{UNIT}/api/caixa/lancar", json={
            "placa": "PIX0001", "cliente": "X",
            "servico": "VISTORIA CAUTELAR", "valor": 100.0, "fp": "pix",
        })
        r = client.post(f"/u/{UNIT}/api/caixa/conferir", json={
            "records": [{
                "id": "rec-pix", "placa": "PIX0001",
                "servico": "VISTORIA CAUTELAR", "preco": 100.0, "fp": "AV",
            }],
        })
        conf = r.get_json()["conferencia"]["rec-pix"]
        assert conf["status"] == "ok"
        assert conf["pdv_cv"] == ""


# ══════════════════════════════════════════════════════════════════════════════
# /api/caixa/conferir devolve FP real do PDV pra planilha AV
# ══════════════════════════════════════════════════════════════════════════════
# Sispevi so manda 'AV' ou 'FA' na planilha. Quem define se eh pix/debito/
# credito/dinheiro eh o PDV. Frontend usa esse pdv_fp pra hidratar
# rec.avPagamento antes de mandar pro Tiny (senao a forma_recebimento vai vazia).

class TestConferirDevolveFPdoPDV:
    @pytest.mark.parametrize("fp_pdv,cv,fp_esperado", [
        ("pix",      "",        "pix"),
        ("debito",   "11111",   "debito"),
        ("credito",  "22222",   "credito"),
        ("dinheiro", "",        "dinheiro"),
    ])
    def test_av_planilha_recebe_fp_real_do_pdv(self, client, fp_pdv, cv, fp_esperado):
        body = {
            "placa": "AV0001", "cliente": "X",
            "servico": "VISTORIA CAUTELAR", "valor": 100.0, "fp": fp_pdv,
        }
        if cv:
            body["cv"] = cv
        client.post(f"/u/{UNIT}/api/caixa/lancar", json=body)
        r = client.post(f"/u/{UNIT}/api/caixa/conferir", json={
            "records": [{
                "id": "rec-av", "placa": "AV0001",
                "servico": "VISTORIA CAUTELAR", "preco": 100.0, "fp": "AV",
            }],
        })
        conf = r.get_json()["conferencia"]["rec-av"]
        assert conf["status"] == "ok"
        assert conf["pdv_fp"] == fp_esperado

    def test_av_planilha_com_pdv_faturado_diverge(self, client):
        """PDV faturado nao deve auto-preencher AV da planilha — eh divergencia."""
        client.post(f"/u/{UNIT}/api/caixa/lancar", json={
            "placa": "AV0002", "cliente": "X",
            "servico": "VISTORIA CAUTELAR", "valor": 100.0, "fp": "faturado",
        })
        r = client.post(f"/u/{UNIT}/api/caixa/conferir", json={
            "records": [{
                "id": "rec-av2", "placa": "AV0002",
                "servico": "VISTORIA CAUTELAR", "preco": 100.0, "fp": "AV",
            }],
        })
        conf = r.get_json()["conferencia"]["rec-av2"]
        assert conf["status"] == "divergencia_fp"
        # pdv_fp ainda devolvido, mas o frontend so propaga em status=ok/ok_fallback
        assert conf["pdv_fp"] == "faturado"


# ══════════════════════════════════════════════════════════════════════════════
# Tiny payload: avPagamento (hidratado do PDV) define forma_recebimento
# ══════════════════════════════════════════════════════════════════════════════
# Confirma end-to-end: planilha vem com fp=AV e avPagamento hidratado pelo
# conferir; Tiny recebe formaRecebimento mapeado pelo avPagamento, nao pelo fp.

class TestTinyPayloadUsaAvPagamento:
    def _cfg(self):
        return {"tiny": {
            "base_url": "x", "token_url": "x", "oauth_scope": "openid",
            "timeout_seconds": 30, "client_id": "a", "client_secret": "b",
            "redirect_uri": "c", "scope": "openid",
            "cliente_ids": {}, "categoria_ids": {},
            "forma_recebimento_ids": {
                "pix": 500, "debito": 501, "credito": 502, "dinheiro": 503,
                "AV": 599, "FA": 600,
            },
            "auto_create_contacts": False, "include_forma_recebimento": True,
            "numero_documento_prefix": "PLA", "default_tipo_pessoa": "J",
            "require_payment_mapping": False,
            "vencimento_tipo": "ultimo_dia_mes", "vencimento_dias": 0,
            "contas_receber_fp": ["AV", "FA"],
            "servico_aliases": {}, "fp_aliases": {}, "cliente_aliases": {},
        }}

    @pytest.mark.parametrize("av_pag,id_esperado", [
        ("pix",      500),
        ("debito",   501),
        ("credito",  502),
        ("dinheiro", 503),
    ])
    def test_formaRecebimento_vem_do_avPagamento(self, tmp_path, av_pag, id_esperado):
        from tiny_import import TinyImporter, NormalizedRecord
        rec = NormalizedRecord(
            data="2026-05-13", modelo="GOL", placa="ABC1234", cliente="X",
            servico="VISTORIA CAUTELAR", fp="AV", preco="100",
            origem_arquivo="t", linha_origem=1, chave_deduplicacao="x",
            av_pagamento=av_pag, cpf="", cv="999",
        )
        imp = TinyImporter(self._cfg(), tmp_path)
        imp.resolve_contact = lambda *a, **k: 999
        p = imp.build_accounts_receivable_payload(rec)
        assert p["formaRecebimento"] == id_esperado
        # AV pago → vencimento = data do servico, nao fim do mes
        assert p["dataVencimento"] == "2026-05-13"

    def test_av_sem_avPagamento_cai_pra_fp_AV(self, tmp_path):
        """Se conferirComPDV nao rodou, avPagamento fica 'pendente' e isTinySendable
        bloqueia. Mas se algum bug deixar passar, FP=AV mapeia pra 599 (fallback)."""
        from tiny_import import TinyImporter, NormalizedRecord
        rec = NormalizedRecord(
            data="2026-05-13", modelo="GOL", placa="ABC1234", cliente="X",
            servico="VISTORIA CAUTELAR", fp="AV", preco="100",
            origem_arquivo="t", linha_origem=1, chave_deduplicacao="x",
            av_pagamento="pendente", cpf="", cv="",
        )
        imp = TinyImporter(self._cfg(), tmp_path)
        imp.resolve_contact = lambda *a, **k: 999
        p = imp.build_accounts_receivable_payload(rec)
        # is_av_paid("pendente")=False → cai no fp=AV → mapeia pra 599
        assert p["formaRecebimento"] == 599
