"""
Testes do endpoint /api/relatorio/vistoriadores e da pagina /vistoriadores.

Cobre:
- Agregacao por perito (qtd, valor, ticket medio, % participacao)
- Filtros (periodo, unidade, servico, perito)
- Comparativo com periodo anterior (variacao %)
- Permissao: master ve todas, operador ve so a dele
- Breakdown por servico
"""
from __future__ import annotations

import os
import sys
import datetime as dt
from pathlib import Path
from unittest.mock import patch

import pytest

os.environ.setdefault("SECRET_KEY", "test-secret-key-pytest")
os.environ.setdefault("USERS_CONFIG", "{}")
os.environ.setdefault("UNITS_CONFIG", "{}")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import server  # noqa: E402
from caixa_db import upsert_vistorias_planilha  # noqa: E402


UNITS_FIX = {
    "sp":     {"nome": "São Paulo"},
    "rj":     {"nome": "Rio de Janeiro"},
}
MASTER_USER = {"email": "m@a.com", "name": "M", "unit": None, "master": True}
OP_SP_USER  = {"email": "o@a.com", "name": "O", "unit": "sp", "master": False}


@pytest.fixture
def setup(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    yield tmp_path


@pytest.fixture
def master_client(setup):
    with patch.object(server, "_current_user", return_value=MASTER_USER):
        with server.app.test_client() as c:
            yield c


@pytest.fixture
def op_client(setup):
    with patch.object(server, "_current_user", return_value=OP_SP_USER):
        with server.app.test_client() as c:
            yield c


def _v(**kw):
    base = {
        "data": "2026-05-13", "placa": "ABC1234", "cliente": "X",
        "servico": "VISTORIA CAUTELAR", "valor": 100.0, "fp": "AV",
        "perito": "JOAO", "arquivo": "p.xls",
    }
    base.update(kw)
    return base


def _seed_sp(setup):
    unit_dir = server._unit_state_dir("sp")
    unit_dir.mkdir(parents=True, exist_ok=True)
    upsert_vistorias_planilha("sp", unit_dir, [
        _v(placa="A001", perito="JOAO", servico="VISTORIA CAUTELAR", valor=100),
        _v(placa="A002", perito="JOAO", servico="VISTORIA CAUTELAR", valor=120),
        _v(placa="A003", perito="JOAO", servico="LAUDO DE TRANSFERENCIA", valor=200),
        _v(placa="A004", perito="MARIA", servico="VISTORIA CAUTELAR", valor=100),
    ])


def _seed_rj(setup):
    unit_dir = server._unit_state_dir("rj")
    unit_dir.mkdir(parents=True, exist_ok=True)
    upsert_vistorias_planilha("rj", unit_dir, [
        _v(placa="R001", perito="PEDRO", servico="VISTORIA CAUTELAR", valor=150),
        _v(placa="R002", perito="PEDRO", servico="VISTORIA CAUTELAR", valor=150),
    ])


# ══════════════════════════════════════════════════════════════════════════════
# Agregacao basica
# ══════════════════════════════════════════════════════════════════════════════

class TestAgregacao:
    def test_ranking_ordenado_por_valor(self, master_client, setup):
        _seed_sp(setup)
        r = master_client.get("/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31&unit=sp")
        body = r.get_json()
        assert body["success"]
        peritos = body["peritos"]
        # JOAO: 100+120+200=420; MARIA: 100
        assert peritos[0]["perito"] == "JOAO"
        assert peritos[0]["valor"] == 420.0
        assert peritos[0]["qtd"]   == 3
        assert peritos[1]["perito"] == "MARIA"
        assert peritos[1]["valor"] == 100.0

    def test_ticket_medio(self, master_client, setup):
        _seed_sp(setup)
        r = master_client.get("/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31&unit=sp")
        joao = next(p for p in r.get_json()["peritos"] if p["perito"] == "JOAO")
        assert joao["ticket_medio"] == 140.0  # 420 / 3

    def test_pct_participacao(self, master_client, setup):
        _seed_sp(setup)
        r = master_client.get("/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31&unit=sp")
        peritos = r.get_json()["peritos"]
        # Total = 520; JOAO 420 = 80.8%, MARIA 100 = 19.2%
        joao = next(p for p in peritos if p["perito"] == "JOAO")
        assert joao["pct_valor"] == 80.8

    def test_breakdown_por_servico(self, master_client, setup):
        _seed_sp(setup)
        r = master_client.get("/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31&unit=sp")
        joao = next(p for p in r.get_json()["peritos"] if p["perito"] == "JOAO")
        # JOAO tem 2 cautelares (220) e 1 transferencia (200)
        servicos = {s["servico"]: s for s in joao["por_servico"]}
        assert servicos["VISTORIA CAUTELAR"]["qtd"] == 2
        assert servicos["VISTORIA CAUTELAR"]["valor"] == 220.0
        assert servicos["LAUDO DE TRANSFERENCIA"]["qtd"] == 1

    def test_totais_geral(self, master_client, setup):
        _seed_sp(setup)
        r = master_client.get("/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31&unit=sp")
        totais = r.get_json()["totais"]
        assert totais["qtd"] == 4
        assert totais["valor"] == 520.0


# ══════════════════════════════════════════════════════════════════════════════
# Permissao
# ══════════════════════════════════════════════════════════════════════════════

class TestPermissao:
    def test_master_ve_todas_unidades(self, master_client, setup):
        _seed_sp(setup)
        _seed_rj(setup)
        r = master_client.get("/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31")
        body = r.get_json()
        peritos = {p["perito"] for p in body["peritos"]}
        assert peritos == {"JOAO", "MARIA", "PEDRO"}

    def test_operador_so_ve_sua_unidade(self, op_client, setup):
        _seed_sp(setup)
        _seed_rj(setup)
        r = op_client.get("/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31")
        body = r.get_json()
        peritos = {p["perito"] for p in body["peritos"]}
        # PEDRO esta em RJ → operador SP nao ve
        assert "PEDRO" not in peritos
        assert "JOAO" in peritos
        assert "MARIA" in peritos

    def test_operador_tentando_filtrar_outra_unidade_bloqueia(self, op_client, setup):
        _seed_sp(setup)
        _seed_rj(setup)
        r = op_client.get("/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31&unit=rj")
        assert r.status_code == 403


# ══════════════════════════════════════════════════════════════════════════════
# Filtros
# ══════════════════════════════════════════════════════════════════════════════

class TestFiltros:
    def test_filtro_servico_cautelar(self, master_client, setup):
        _seed_sp(setup)
        r = master_client.get(
            "/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31&unit=sp&servico=CAUTELAR"
        )
        body = r.get_json()
        # So conta cautelares: JOAO 2x100+120=220, MARIA 1x100
        joao = next(p for p in body["peritos"] if p["perito"] == "JOAO")
        assert joao["qtd"] == 2
        assert joao["valor"] == 220.0

    def test_filtro_perito_especifico(self, master_client, setup):
        _seed_sp(setup)
        r = master_client.get(
            "/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31&unit=sp&perito=MARIA"
        )
        peritos = r.get_json()["peritos"]
        assert len(peritos) == 1
        assert peritos[0]["perito"] == "MARIA"

    def test_periodo_vazio_sem_dados(self, master_client, setup):
        _seed_sp(setup)
        r = master_client.get(
            "/api/relatorio/vistoriadores?inicio=2026-06-01&fim=2026-06-30"
        )
        body = r.get_json()
        assert body["peritos"] == []
        assert body["totais"]["qtd"] == 0


# ══════════════════════════════════════════════════════════════════════════════
# Comparativo periodo anterior
# ══════════════════════════════════════════════════════════════════════════════

class TestComparativo:
    def test_comparativo_inclui_periodo_anterior(self, master_client, setup):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        # Abril (anterior): JOAO 200, MARIA 100
        upsert_vistorias_planilha("sp", unit_dir, [
            _v(data="2026-04-10", placa="X001", perito="JOAO", valor=200),
            _v(data="2026-04-15", placa="X002", perito="MARIA", valor=100),
        ])
        # Maio (atual): JOAO 420 (3 vistorias), MARIA 100
        _seed_sp(setup)

        r = master_client.get(
            "/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31&unit=sp&comparativo=1"
        )
        body = r.get_json()
        assert "periodo_anterior" in body
        joao = next(p for p in body["peritos"] if p["perito"] == "JOAO")
        assert joao["anterior"]["valor"] == 200.0
        # JOAO foi 200 → 420, variacao = +110%
        assert joao["var_valor_pct"] == 110.0


# ══════════════════════════════════════════════════════════════════════════════
# /api/units-list
# ══════════════════════════════════════════════════════════════════════════════

class TestUnitsList:
    def test_master_ve_todas(self, master_client, setup):
        r = master_client.get("/api/units-list")
        body = r.get_json()
        slugs = {u["slug"] for u in body["units"]}
        assert slugs == {"sp", "rj"}

    def test_operador_ve_so_sua(self, op_client, setup):
        r = op_client.get("/api/units-list")
        body = r.get_json()
        slugs = {u["slug"] for u in body["units"]}
        assert slugs == {"sp"}


# ══════════════════════════════════════════════════════════════════════════════
# Pagina HTML serve
# ══════════════════════════════════════════════════════════════════════════════

class TestPagina:
    def test_vistoriadores_html_serve(self, master_client):
        r = master_client.get("/vistoriadores")
        assert r.status_code == 200
        # Conteudo basico esperado
        assert b"Vistoriadores" in r.data
