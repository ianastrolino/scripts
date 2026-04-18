"""
Testes de integração para as rotas críticas do Caixa do Dia.

Estratégia:
- Flask test client (sem rede, sem Tiny, sem Railway)
- DATA_DIR apontado para tmp_path do pytest (limpo a cada teste)
- _current_user patcheado para simular sessão válida
- UNITS injetado via server.UNITS direto (sem env var)
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Configura env antes de importar server
os.environ.setdefault("SECRET_KEY", "test-secret-key-pytest")
os.environ.setdefault("USERS_CONFIG", "{}")
os.environ.setdefault("UNITS_CONFIG", '{"testunit": {"nome": "Test Unit", "master_pin": "1234"}}')

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import server  # noqa: E402

UNIT = "testunit"
FAKE_USER = {"email": "op@astrovistorias.com.br", "name": "Operador", "unit": UNIT, "master": False}


@pytest.fixture
def client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = {"testunit": {"nome": "Test Unit", "master_pin": "1234"}}
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=FAKE_USER):
        with server.app.test_client() as c:
            yield c


# ── helpers ───────────────────────────────────────────────────────────────────

def _lancar(client, **kwargs):
    payload = {"placa": "ABC1234", "cliente": "CLIENTE TESTE", "servico": "LAUDO", "valor": 150.0, "fp": "pix", **kwargs}
    return client.post(f"/u/{UNIT}/api/caixa/lancar", json=payload)


# ══════════════════════════════════════════════════════════════════════════════
# lancar
# ══════════════════════════════════════════════════════════════════════════════

class TestLancar:
    def test_ok(self, client):
        r = _lancar(client)
        assert r.status_code == 200
        body = r.get_json()
        assert body["success"] is True
        assert body["lancamento"]["placa"] == "ABC1234"
        assert body["lancamento"]["fp"] == "pix"
        assert body["totais"]["pix"] == 150.0
        assert body["total_lancamentos"] == 1

    def test_segundo_lancamento_acumula_total(self, client):
        _lancar(client, valor=100.0, fp="dinheiro")
        r = _lancar(client, valor=200.0, fp="dinheiro")
        assert r.get_json()["totais"]["dinheiro"] == 300.0

    def test_sem_placa_retorna_400(self, client):
        r = _lancar(client, placa="")
        assert r.status_code == 400
        assert "Placa" in r.get_json()["error"]

    def test_sem_cliente_retorna_400(self, client):
        r = _lancar(client, cliente="")
        assert r.status_code == 400

    def test_valor_zero_retorna_400(self, client):
        r = _lancar(client, valor=0)
        assert r.status_code == 400

    def test_valor_negativo_retorna_400(self, client):
        r = _lancar(client, valor=-50)
        assert r.status_code == 400

    def test_fp_invalido_retorna_400(self, client):
        r = _lancar(client, fp="transferencia")
        assert r.status_code == 400

    def test_placa_uppercase(self, client):
        r = _lancar(client, placa="abc1234")
        assert r.get_json()["lancamento"]["placa"] == "ABC1234"

    def test_persiste_em_disco(self, client, tmp_path):
        import sqlite3
        _lancar(client)
        db_path = tmp_path / UNIT / "caixa_dia.db"
        assert db_path.exists()
        with sqlite3.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT * FROM lancamentos").fetchall()
        assert len(rows) == 1


# ══════════════════════════════════════════════════════════════════════════════
# estado
# ══════════════════════════════════════════════════════════════════════════════

class TestEstado:
    def test_vazio_sem_lancamentos(self, client):
        r = client.get(f"/u/{UNIT}/api/caixa/estado")
        assert r.status_code == 200
        body = r.get_json()
        assert body["success"] is True
        assert body["lancamentos"] == []
        assert body["totais"]["total"] == 0.0

    def test_retorna_lancamentos_do_dia(self, client):
        _lancar(client, valor=100.0, fp="pix")
        _lancar(client, valor=50.0, fp="dinheiro")
        body = client.get(f"/u/{UNIT}/api/caixa/estado").get_json()
        assert len(body["lancamentos"]) == 2
        assert body["totais"]["pix"] == 100.0
        assert body["totais"]["dinheiro"] == 50.0
        assert body["totais"]["total"] == 150.0

    def test_campos_obrigatorios_presentes(self, client):
        _lancar(client)
        lc = client.get(f"/u/{UNIT}/api/caixa/estado").get_json()["lancamentos"][0]
        for campo in ("id", "hora", "timestamp", "placa", "cliente", "servico", "valor", "fp"):
            assert campo in lc, f"campo ausente: {campo}"

    def test_nao_expoe_colunas_internas(self, client):
        _lancar(client)
        lc = client.get(f"/u/{UNIT}/api/caixa/estado").get_json()["lancamentos"][0]
        assert "unit" not in lc
        assert "data" not in lc


# ══════════════════════════════════════════════════════════════════════════════
# editar
# ══════════════════════════════════════════════════════════════════════════════

class TestEditar:
    def _editar(self, client, lancamento_id: str, pin: str = "1234", **kwargs):
        payload = {
            "pin": pin,
            "placa": "XYZ9999", "cliente": "NOVO CLIENTE",
            "servico": "VISTORIA CAUTELAR", "valor": 200.0, "fp": "debito",
            **kwargs,
        }
        return client.put(f"/u/{UNIT}/api/caixa/editar/{lancamento_id}", json=payload)

    def test_ok(self, client):
        lc_id = _lancar(client).get_json()["lancamento"]["id"]
        r = self._editar(client, lc_id)
        assert r.status_code == 200
        body = r.get_json()
        assert body["success"] is True
        assert body["totais"]["debito"] == 200.0

    def test_editar_para_faturado(self, client):
        lc_id = _lancar(client, fp="pix", valor=150.0).get_json()["lancamento"]["id"]
        r = self._editar(client, lc_id, fp="faturado", valor=150.0)
        assert r.status_code == 200
        assert r.get_json()["totais"]["faturado"] == 150.0

    def test_editar_atualiza_valor_nos_totais(self, client):
        _lancar(client, valor=100.0, fp="pix")
        lc_id = _lancar(client, valor=50.0, fp="pix").get_json()["lancamento"]["id"]
        self._editar(client, lc_id, fp="pix", valor=80.0)
        body = client.get(f"/u/{UNIT}/api/caixa/estado").get_json()
        assert body["totais"]["pix"] == 180.0

    def test_pin_errado_retorna_403(self, client):
        lc_id = _lancar(client).get_json()["lancamento"]["id"]
        r = self._editar(client, lc_id, pin="0000")
        assert r.status_code == 403

    def test_id_inexistente_retorna_404(self, client):
        r = self._editar(client, "id-que-nao-existe")
        assert r.status_code == 404

    def test_fp_invalido_retorna_400(self, client):
        lc_id = _lancar(client).get_json()["lancamento"]["id"]
        r = self._editar(client, lc_id, fp="boleto")
        assert r.status_code == 400

    def test_valor_zero_retorna_400(self, client):
        lc_id = _lancar(client).get_json()["lancamento"]["id"]
        r = self._editar(client, lc_id, valor=0)
        assert r.status_code == 400


# ══════════════════════════════════════════════════════════════════════════════
# excluir
# ══════════════════════════════════════════════════════════════════════════════

class TestExcluir:
    def _excluir(self, client, lancamento_id: str, pin: str = "1234"):
        return client.delete(
            f"/u/{UNIT}/api/caixa/excluir/{lancamento_id}",
            json={"pin": pin},
        )

    def test_ok(self, client):
        lc_id = _lancar(client).get_json()["lancamento"]["id"]
        r = self._excluir(client, lc_id)
        assert r.status_code == 200
        body = r.get_json()
        assert body["success"] is True
        assert body["total_lancamentos"] == 0

    def test_totais_atualizados_apos_exclusao(self, client):
        _lancar(client, valor=100.0, fp="pix")
        lc_id = _lancar(client, valor=50.0, fp="pix").get_json()["lancamento"]["id"]
        r = self._excluir(client, lc_id)
        assert r.get_json()["totais"]["pix"] == 100.0

    def test_pin_errado_retorna_403(self, client):
        lc_id = _lancar(client).get_json()["lancamento"]["id"]
        r = self._excluir(client, lc_id, pin="0000")
        assert r.status_code == 403

    def test_id_inexistente_retorna_404(self, client):
        _lancar(client)
        r = self._excluir(client, "id-que-nao-existe")
        assert r.status_code == 404

    def test_pin_vazio_retorna_403(self, client):
        lc_id = _lancar(client).get_json()["lancamento"]["id"]
        r = self._excluir(client, lc_id, pin="")
        assert r.status_code == 403


# ══════════════════════════════════════════════════════════════════════════════
# conferir (cruzamento PDV × planilha — equivale ao "fechar")
# ══════════════════════════════════════════════════════════════════════════════

class TestConferir:
    def _conferir(self, client, records: list):
        return client.post(f"/u/{UNIT}/api/caixa/conferir", json={"records": records})

    def test_match_exato(self, client):
        _lancar(client, placa="ABC1234", servico="LAUDO DE TRANSFERENCIA", valor=150.0, fp="pix")
        records = [{"id": "r1", "placa": "ABC1234", "servico": "LAUDO DE TRANSFERENCIA", "preco": 150.0, "fp": "AV"}]
        body = self._conferir(client, records).get_json()
        assert body["success"] is True
        assert body["conferencia"]["r1"]["status"] == "ok"

    def test_divergencia_valor(self, client):
        _lancar(client, placa="ABC1234", servico="LAUDO DE TRANSFERENCIA", valor=100.0, fp="pix")
        records = [{"id": "r1", "placa": "ABC1234", "servico": "LAUDO DE TRANSFERENCIA", "preco": 150.0, "fp": "AV"}]
        body = self._conferir(client, records).get_json()
        assert body["conferencia"]["r1"]["status"] == "divergencia_valor"
        assert body["conferencia"]["r1"]["pdv_valor"] == 100.0

    def test_sem_pdv(self, client):
        records = [{"id": "r1", "placa": "XYZ9999", "servico": "LAUDO DE TRANSFERENCIA", "preco": 150.0, "fp": "AV"}]
        body = self._conferir(client, records).get_json()
        assert body["conferencia"]["r1"]["status"] == "sem_pdv"

    def test_ignora_fp_nao_av(self, client):
        records = [{"id": "r1", "placa": "ABC1234", "servico": "LAUDO", "preco": 150.0, "fp": "PIX"}]
        body = self._conferir(client, records).get_json()
        assert "r1" not in body["conferencia"]

    def test_pdv_sem_planilha_retornado(self, client):
        _lancar(client, placa="ZZZ0001", servico="PESQUISA AVULSA", valor=50.0, fp="dinheiro")
        body = self._conferir(client, []).get_json()
        assert any(p["placa"] == "ZZZ0001" for p in body["pdv_sem_planilha"])

    def test_placa_case_insensitive(self, client):
        _lancar(client, placa="ABC1234", servico="LAUDO DE TRANSFERENCIA", valor=150.0, fp="pix")
        records = [{"id": "r1", "placa": "abc1234", "servico": "laudo de transferencia", "preco": 150.0, "fp": "AV"}]
        body = self._conferir(client, records).get_json()
        assert body["conferencia"]["r1"]["status"] == "ok"


# ══════════════════════════════════════════════════════════════════════════════
# migração JSON → SQLite
# ══════════════════════════════════════════════════════════════════════════════

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from caixa_db import migrate_from_json, load_lancamentos, _connect  # noqa: E402


class TestMigracao:
    def test_migracao_json_para_sqlite(self, tmp_path):
        unit_dir = tmp_path / "u1"
        unit_dir.mkdir()
        (unit_dir / "caixa_dia.json").write_text(json.dumps({
            "data": "2026-04-17",
            "lancamentos": [{
                "id": "aa11bb22", "hora": "10:00",
                "timestamp": "2026-04-17T10:00:00-03:00",
                "placa": "ABC1234", "cliente": "CLI", "servico": "LAUDO",
                "valor": 100.0, "fp": "pix",
            }],
        }))
        n = migrate_from_json("u1", unit_dir)
        assert n == 1
        rows = load_lancamentos("u1", unit_dir, "2026-04-17")
        assert len(rows) == 1
        assert rows[0]["placa"] == "ABC1234"

    def test_migracao_idempotente(self, tmp_path):
        unit_dir = tmp_path / "u1"
        unit_dir.mkdir()
        _connect(unit_dir).close()  # cria .db vazio
        n = migrate_from_json("u1", unit_dir)
        assert n == 0

    def test_migracao_json_corrompido_limpa_db(self, tmp_path):
        unit_dir = tmp_path / "u1"
        unit_dir.mkdir()
        (unit_dir / "caixa_dia.json").write_text("NOT JSON")
        with pytest.raises(Exception):
            migrate_from_json("u1", unit_dir)
        assert not (unit_dir / "caixa_dia.db").exists()

    def test_acumulacao_persiste_em_sqlite(self, client, tmp_path):
        _lancar(client, valor=100.0, fp="pix")
        _lancar(client, valor=50.0, fp="dinheiro")
        db = tmp_path / UNIT / "caixa_dia.db"
        with sqlite3.connect(str(db)) as conn:
            total = conn.execute("SELECT COUNT(*) FROM lancamentos").fetchone()[0]
        assert total == 2
