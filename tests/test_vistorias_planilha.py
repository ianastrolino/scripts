"""
Testes da tabela vistorias_planilha — base do relatorio de vistoriadores
(aba Vistoriadores no SAAS).

Independente de envios_erp: registra vistorias da planilha importada mesmo
que nunca cheguem ao ERP. Alimentada via /api/snapshot.
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
from caixa_db import (  # noqa: E402
    upsert_vistorias_planilha,
    load_vistorias_planilha,
    _connect,
)


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


@pytest.fixture
def unit_dir(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    d = server._unit_state_dir("sp")
    d.mkdir(parents=True, exist_ok=True)
    return d


# ══════════════════════════════════════════════════════════════════════════════
# upsert_vistorias_planilha
# ══════════════════════════════════════════════════════════════════════════════

def _v(**kw):
    base = {
        "data": "2026-05-13", "placa": "ABC1234", "cliente": "TESTE",
        "servico": "VISTORIA CAUTELAR", "valor": 100.0, "fp": "AV",
        "perito": "JOAO SILVA", "arquivo": "planilha.xls",
    }
    base.update(kw)
    return base


class TestUpsertVistorias:
    def test_insere_nova(self, unit_dir):
        r = upsert_vistorias_planilha("sp", unit_dir, [_v()])
        assert r == {"inseridas": 1, "atualizadas": 0}
        rows = load_vistorias_planilha("sp", unit_dir, "2026-05-01", "2026-05-31")
        assert len(rows) == 1
        assert rows[0]["perito"] == "JOAO SILVA"

    def test_reimport_mesma_vistoria_atualiza(self, unit_dir):
        upsert_vistorias_planilha("sp", unit_dir, [_v(perito="JOAO")])
        # Reimport com perito diferente — atualiza
        r = upsert_vistorias_planilha("sp", unit_dir, [_v(perito="MARIA")])
        assert r == {"inseridas": 0, "atualizadas": 1}
        rows = load_vistorias_planilha("sp", unit_dir, "2026-05-01", "2026-05-31")
        assert len(rows) == 1
        assert rows[0]["perito"] == "MARIA"

    def test_reimport_perito_vazio_nao_apaga(self, unit_dir):
        upsert_vistorias_planilha("sp", unit_dir, [_v(perito="JOAO")])
        upsert_vistorias_planilha("sp", unit_dir, [_v(perito="")])
        rows = load_vistorias_planilha("sp", unit_dir, "2026-05-01", "2026-05-31")
        assert rows[0]["perito"] == "JOAO"

    def test_diferentes_placas_inserem_separado(self, unit_dir):
        upsert_vistorias_planilha("sp", unit_dir, [
            _v(placa="AAA1111"),
            _v(placa="BBB2222"),
        ])
        rows = load_vistorias_planilha("sp", unit_dir, "2026-05-01", "2026-05-31")
        assert len(rows) == 2

    def test_mesma_placa_servicos_diferentes_inserem_separado(self, unit_dir):
        upsert_vistorias_planilha("sp", unit_dir, [
            _v(servico="VISTORIA CAUTELAR"),
            _v(servico="LAUDO DE TRANSFERENCIA"),
        ])
        rows = load_vistorias_planilha("sp", unit_dir, "2026-05-01", "2026-05-31")
        assert len(rows) == 2

    def test_normaliza_uppercase(self, unit_dir):
        upsert_vistorias_planilha("sp", unit_dir, [_v(
            placa="abc1234", cliente="teste", servico="vistoria cautelar",
            perito="joao silva",
        )])
        rows = load_vistorias_planilha("sp", unit_dir, "2026-05-01", "2026-05-31")
        assert rows[0]["placa"] == "ABC1234"
        assert rows[0]["cliente"] == "TESTE"
        assert rows[0]["servico"] == "VISTORIA CAUTELAR"
        assert rows[0]["perito"] == "JOAO SILVA"

    def test_skip_malformados(self, unit_dir):
        # Sem placa → ignora
        r = upsert_vistorias_planilha("sp", unit_dir, [
            _v(placa=""),
            _v(),
        ])
        assert r == {"inseridas": 1, "atualizadas": 0}

    def test_valor_atualiza_se_maior_que_zero(self, unit_dir):
        upsert_vistorias_planilha("sp", unit_dir, [_v(valor=100)])
        upsert_vistorias_planilha("sp", unit_dir, [_v(valor=150)])
        rows = load_vistorias_planilha("sp", unit_dir, "2026-05-01", "2026-05-31")
        assert rows[0]["valor"] == 150.0


# ══════════════════════════════════════════════════════════════════════════════
# load_vistorias_planilha — filtros
# ══════════════════════════════════════════════════════════════════════════════

class TestLoadVistorias:
    @pytest.fixture(autouse=True)
    def _seed(self, unit_dir):
        upsert_vistorias_planilha("sp", unit_dir, [
            _v(data="2026-05-01", placa="A001", perito="JOAO"),
            _v(data="2026-05-15", placa="A002", perito="MARIA"),
            _v(data="2026-05-15", placa="A003", servico="LAUDO DE TRANSFERENCIA", perito="JOAO"),
            _v(data="2026-04-30", placa="A004", perito="MARIA"),  # fora do mes
        ])
        self.unit_dir = unit_dir

    def test_filtro_periodo(self):
        rows = load_vistorias_planilha("sp", self.unit_dir, "2026-05-01", "2026-05-31")
        assert len(rows) == 3  # A004 esta em abril

    def test_filtro_perito(self):
        rows = load_vistorias_planilha("sp", self.unit_dir, "2026-05-01", "2026-05-31", perito="JOAO")
        assert len(rows) == 2
        assert all(r["perito"] == "JOAO" for r in rows)

    def test_filtro_servico_substring(self):
        rows = load_vistorias_planilha("sp", self.unit_dir, "2026-05-01", "2026-05-31", servico="CAUTELAR")
        assert len(rows) == 2  # 2 cautelares em maio

    def test_periodo_vazio_retorna_lista_vazia(self):
        rows = load_vistorias_planilha("sp", self.unit_dir, "2026-06-01", "2026-06-30")
        assert rows == []


# ══════════════════════════════════════════════════════════════════════════════
# Integracao com /api/snapshot — popula vistorias_planilha automatico
# ══════════════════════════════════════════════════════════════════════════════

class TestSnapshotAlimentaVistorias:
    def test_snapshot_grava_em_vistorias_planilha(self, client, unit_dir):
        r = client.post("/u/sp/api/snapshot", json={
            "data": "2026-05-13",
            "arquivos": ["planilha_13.xls"],
            "records": [
                {"data": "2026-05-13", "placa": "ABC1234", "cliente": "X",
                 "servico": "VISTORIA CAUTELAR", "preco": 100, "fp": "AV",
                 "perito": "JOAO SILVA", "origemArquivo": "planilha_13.xls"},
                {"data": "2026-05-13", "placa": "XYZ9999", "cliente": "Y",
                 "servico": "LAUDO DE TRANSFERENCIA", "preco": 200, "fp": "FA",
                 "perito": "MARIA", "origemArquivo": "planilha_13.xls"},
            ],
        })
        assert r.status_code == 200
        rows = load_vistorias_planilha("sp", unit_dir, "2026-05-13", "2026-05-13")
        assert len(rows) == 2
        peritos = {row["perito"] for row in rows}
        assert peritos == {"JOAO SILVA", "MARIA"}

    def test_snapshot_ignora_pdvExtra(self, client, unit_dir):
        client.post("/u/sp/api/snapshot", json={
            "data": "2026-05-13",
            "arquivos": ["planilha.xls"],
            "records": [
                {"data": "2026-05-13", "placa": "ABC1234", "cliente": "X",
                 "servico": "VISTORIA CAUTELAR", "preco": 100, "fp": "AV",
                 "perito": "JOAO", "origemArquivo": "planilha.xls"},
                # PDV extra — nao entra (e PDV avulso, fora do escopo)
                {"data": "2026-05-13", "placa": "PDV0001", "cliente": "X",
                 "servico": "PESQUISA AVULSA", "preco": 50, "fp": "AV",
                 "pdvExtra": True, "perito": ""},
            ],
        })
        rows = load_vistorias_planilha("sp", unit_dir, "2026-05-13", "2026-05-13")
        assert len(rows) == 1
        assert rows[0]["placa"] == "ABC1234"

    def test_snapshot_ignora_records_marcados_ignorar(self, client, unit_dir):
        client.post("/u/sp/api/snapshot", json={
            "data": "2026-05-13",
            "arquivos": ["planilha.xls"],
            "records": [
                {"data": "2026-05-13", "placa": "ABC1234", "cliente": "X",
                 "servico": "VISTORIA CAUTELAR", "preco": 100, "fp": "AV",
                 "perito": "JOAO", "origemArquivo": "planilha.xls"},
                {"data": "2026-05-13", "placa": "BBB1111", "cliente": "Y",
                 "servico": "VISTORIA CAUTELAR", "preco": 100, "fp": "AV",
                 "perito": "MARIA", "ignorar": True, "origemArquivo": "planilha.xls"},
            ],
        })
        rows = load_vistorias_planilha("sp", unit_dir, "2026-05-13", "2026-05-13")
        assert len(rows) == 1

    def test_reimport_snapshot_dedup(self, client, unit_dir):
        """Operador importa planilha 2x — vistorias_planilha nao duplica."""
        payload = {
            "data": "2026-05-13",
            "arquivos": ["planilha.xls"],
            "records": [
                {"data": "2026-05-13", "placa": "ABC1234", "cliente": "X",
                 "servico": "VISTORIA CAUTELAR", "preco": 100, "fp": "AV",
                 "perito": "JOAO", "origemArquivo": "planilha.xls"},
            ],
        }
        client.post("/u/sp/api/snapshot", json=payload)
        client.post("/u/sp/api/snapshot", json=payload)
        rows = load_vistorias_planilha("sp", unit_dir, "2026-05-13", "2026-05-13")
        assert len(rows) == 1
