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
