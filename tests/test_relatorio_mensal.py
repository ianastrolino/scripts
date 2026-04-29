"""
Testes do endpoint /master/api/relatorio-mensal.

Cobre:
- Classificacao Motor (LAUDO DE TRANSFERENCIA) vs Cautelar (resto)
- Agregacao por unidade × mes
- Totais da rede
- Filtro de mes (?mes=YYYY-MM)
- Master_required
"""
from __future__ import annotations

import datetime as dt
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
from caixa_db import insert_envio_tiny  # noqa: E402

UNITS_FIX = {
    "barueri": {"nome": "Barueri",  "master_pin": "0000"},
    "mooca":   {"nome": "Mooca",    "master_pin": "0000"},
    "moema":   {"nome": "Moema",    "master_pin": "0000"},
}

MASTER_USER = {"email": "admin@astro.com", "name": "Admin", "unit": None, "master": True}
OPERATOR_USER = {"email": "op@astro.com", "name": "Op", "unit": "barueri", "master": False}


def _envio(unit: str, data: str, servico: str, valor: float, suffix: str = "") -> dict:
    """Helper: cria payload de envio_tiny valido pra teste."""
    return {
        "chave_deduplicacao": f"{unit}-{data}-{servico}-{valor}-{suffix}",
        "timestamp":          f"{data}T10:00:00",
        "data_lancamento":    data,
        "placa":              "ABC1234",
        "cliente":            "TESTE",
        "servico":            servico,
        "valor":              valor,
        "fp":                 "pix",
        "status":             "ok",
    }


@pytest.fixture
def master_client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=MASTER_USER):
        with server.app.test_client() as c:
            yield c


@pytest.fixture
def operator_client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=OPERATOR_USER):
        with server.app.test_client() as c:
            yield c


def _seed(unit: str, envios: list[dict]) -> None:
    unit_dir = server._unit_state_dir(unit)
    unit_dir.mkdir(parents=True, exist_ok=True)
    for e in envios:
        insert_envio_tiny(unit, unit_dir, e)


# ══════════════════════════════════════════════════════════════════════════════
# Classificacao Motor vs Cautelar
# ══════════════════════════════════════════════════════════════════════════════

class TestClassificacao:
    def test_laudo_transferencia_eh_motor(self):
        assert server._classifica_servico_relatorio("LAUDO DE TRANSFERENCIA") == "motor"

    def test_vistoria_cautelar_eh_cautelar(self):
        assert server._classifica_servico_relatorio("VISTORIA CAUTELAR") == "cautelar"

    def test_laudo_cautelar_verificacao_eh_cautelar(self):
        assert server._classifica_servico_relatorio("LAUDO CAUTELAR VERIFICACAO") == "cautelar"

    def test_cautelar_com_analise_eh_cautelar(self):
        assert server._classifica_servico_relatorio("CAUTELAR COM ANALISE") == "cautelar"

    def test_laudo_de_verificacao_eh_cautelar(self):
        assert server._classifica_servico_relatorio("LAUDO DE VERIFICACAO") == "cautelar"

    def test_servico_desconhecido_eh_cautelar(self):
        # Decisao: tudo que nao eh transferencia vai pra cautelar (Ian unificou)
        assert server._classifica_servico_relatorio("SERVICO DESCONHECIDO") == "cautelar"

    def test_aceita_lowercase_e_espacos(self):
        assert server._classifica_servico_relatorio("  laudo de transferencia  ") == "motor"


# ══════════════════════════════════════════════════════════════════════════════
# Endpoint /master/api/relatorio-mensal
# ══════════════════════════════════════════════════════════════════════════════

class TestRelatorioMensal:
    def test_sem_dados_retorna_zeros(self, master_client):
        body = master_client.get("/master/api/relatorio-mensal").get_json()
        assert body["success"] is True
        assert body["totais"]["motor_qtd"] == 0
        assert body["totais"]["cautelar_qtd"] == 0
        assert body["totais"]["total_qtd"] == 0
        assert body["totais"]["total_valor"] == 0.0

    def test_lista_todas_unidades(self, master_client):
        body = master_client.get("/master/api/relatorio-mensal").get_json()
        nomes = {u["nome"] for u in body["por_unidade"]}
        assert nomes == {"Barueri", "Mooca", "Moema"}

    def test_motor_separado_de_cautelar(self, master_client):
        hoje = dt.date.today().isoformat()
        _seed("barueri", [
            _envio("barueri", hoje, "LAUDO DE TRANSFERENCIA", 80.0, "1"),
            _envio("barueri", hoje, "LAUDO DE TRANSFERENCIA", 80.0, "2"),
            _envio("barueri", hoje, "VISTORIA CAUTELAR", 100.0, "3"),
        ])
        body = master_client.get("/master/api/relatorio-mensal").get_json()
        bar = next(u for u in body["por_unidade"] if u["unit"] == "barueri")
        assert bar["motor_qtd"] == 2
        assert bar["motor_valor"] == 160.0
        assert bar["cautelar_qtd"] == 1
        assert bar["cautelar_valor"] == 100.0
        assert bar["total_qtd"] == 3
        assert bar["total_valor"] == 260.0

    def test_ticket_medio_por_unidade(self, master_client):
        hoje = dt.date.today().isoformat()
        _seed("mooca", [
            _envio("mooca", hoje, "LAUDO DE TRANSFERENCIA", 100.0, "1"),
            _envio("mooca", hoje, "VISTORIA CAUTELAR", 200.0, "2"),
        ])
        body = master_client.get("/master/api/relatorio-mensal").get_json()
        mooca = next(u for u in body["por_unidade"] if u["unit"] == "mooca")
        assert mooca["ticket_medio"] == 150.0  # (100 + 200) / 2

    def test_totais_da_rede_somam_unidades(self, master_client):
        hoje = dt.date.today().isoformat()
        _seed("barueri", [_envio("barueri", hoje, "LAUDO DE TRANSFERENCIA", 80.0, "1")])
        _seed("mooca",   [_envio("mooca",   hoje, "VISTORIA CAUTELAR",      100.0, "1")])
        body = master_client.get("/master/api/relatorio-mensal").get_json()
        t = body["totais"]
        assert t["motor_qtd"] == 1
        assert t["motor_valor"] == 80.0
        assert t["cautelar_qtd"] == 1
        assert t["cautelar_valor"] == 100.0
        assert t["total_qtd"] == 2
        assert t["total_valor"] == 180.0
        assert t["ticket_medio"] == 90.0  # (80 + 100) / 2

    def test_filtro_de_mes_passado_isola_dados(self, master_client):
        hoje = dt.date.today()
        # Envio em mes anterior
        mes_passado = (hoje.replace(day=1) - dt.timedelta(days=1)).isoformat()
        _seed("barueri", [_envio("barueri", mes_passado, "LAUDO DE TRANSFERENCIA", 999.0, "passado")])
        # Envio no mes corrente
        _seed("barueri", [_envio("barueri", hoje.isoformat(), "LAUDO DE TRANSFERENCIA", 50.0, "atual")])

        body = master_client.get("/master/api/relatorio-mensal").get_json()
        bar = next(u for u in body["por_unidade"] if u["unit"] == "barueri")
        # mes corrente: so o de 50.0
        assert bar["motor_qtd"] == 1
        assert bar["motor_valor"] == 50.0

    def test_envios_falha_excluidos(self, master_client):
        hoje = dt.date.today().isoformat()
        # Um envio normal e um com status=falha
        _seed("barueri", [
            _envio("barueri", hoje, "LAUDO DE TRANSFERENCIA", 100.0, "ok"),
            {**_envio("barueri", hoje, "LAUDO DE TRANSFERENCIA", 200.0, "falha"), "status": "falha"},
        ])
        body = master_client.get("/master/api/relatorio-mensal").get_json()
        bar = next(u for u in body["por_unidade"] if u["unit"] == "barueri")
        # So o ok conta — falha excluida pelo _db_load_envios_range
        assert bar["motor_qtd"] == 1
        assert bar["motor_valor"] == 100.0

    def test_periodo_no_payload(self, master_client):
        body = master_client.get("/master/api/relatorio-mensal?mes=2026-04").get_json()
        assert body["mes"] == "2026-04"
        assert body["periodo"]["de"] == "2026-04-01"
        assert body["periodo"]["ate"] == "2026-04-30"

    def test_mes_dezembro_calcula_ultimo_dia_corretamente(self, master_client):
        body = master_client.get("/master/api/relatorio-mensal?mes=2025-12").get_json()
        assert body["periodo"]["de"]  == "2025-12-01"
        assert body["periodo"]["ate"] == "2025-12-31"

    def test_mes_invalido_volta_pro_mes_atual(self, master_client):
        body = master_client.get("/master/api/relatorio-mensal?mes=invalido").get_json()
        hoje = dt.date.today()
        assert body["mes"] == f"{hoje.year:04d}-{hoje.month:02d}"

    def test_unidades_ordenadas_por_nome(self, master_client):
        body = master_client.get("/master/api/relatorio-mensal").get_json()
        nomes = [u["nome"] for u in body["por_unidade"]]
        assert nomes == sorted(nomes, key=str.lower)


# ══════════════════════════════════════════════════════════════════════════════
# Auth
# ══════════════════════════════════════════════════════════════════════════════

class TestAuth:
    def test_operador_recebe_403(self, operator_client):
        r = operator_client.get("/master/api/relatorio-mensal")
        assert r.status_code == 403

    def test_anonimo_recebe_401(self, tmp_path):
        server.DATA_DIR = tmp_path
        server.UNITS = UNITS_FIX
        server.app.config["TESTING"] = True
        with patch.object(server, "_current_user", return_value=None):
            with server.app.test_client() as c:
                r = c.get("/master/api/relatorio-mensal")
        assert r.status_code == 401

    def test_pagina_html_serve_pra_master(self, master_client):
        r = master_client.get("/master/relatorio-mensal")
        assert r.status_code == 200
        assert b"Relat" in r.data  # "Relatorio mensal"
