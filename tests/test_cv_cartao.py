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
