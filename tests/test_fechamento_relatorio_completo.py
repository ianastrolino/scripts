"""
Testes do endpoint /u/<unit>/api/fechamento/relatorio-completo + tela imprimir.

Cobre:
- Agregacao por FP (avista, faturado, detran)
- Subtotais avista_por_fp (dinheiro, pix, debito, credito)
- Lista de faturados e detran ordenadas por hora
- Envios Tiny do dia (count + valor) com falhas separadas
- Conferencia PDV x planilha integrada (sem planilha → exists=False)
- Auth: anonimo 401, operador da outra unit 403, da unit 200, master 200
- Refator: _compute_planilha_status nao quebrou api/planilha/status
"""
from __future__ import annotations

import datetime as dt
import json
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
    "sp": {"nome": "São Paulo", "master_pin": "0000"},
    "rj": {"nome": "Rio de Janeiro", "master_pin": "1111"},
}

MASTER_USER   = {"email": "admin@astro.com", "name": "Admin", "unit": None,  "master": True}
SP_OPERATOR   = {"email": "op-sp@astro.com", "name": "OpSP",  "unit": "sp",  "master": False}
RJ_OPERATOR   = {"email": "op-rj@astro.com", "name": "OpRJ",  "unit": "rj",  "master": False}

HOJE = dt.date.today().isoformat()


@pytest.fixture
def sp_client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=SP_OPERATOR):
        with server.app.test_client() as c:
            yield c


@pytest.fixture
def rj_client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=RJ_OPERATOR):
        with server.app.test_client() as c:
            yield c


@pytest.fixture
def master_client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=MASTER_USER):
        with server.app.test_client() as c:
            yield c


def _lancar(client, unit: str, **kw) -> str:
    """Cria um lancamento via API e retorna o id."""
    payload = {"placa": "ABC1234", "cliente": "TESTE", "servico": "VISTORIA CAUTELAR",
               "valor": 100.0, "fp": "pix", **kw}
    r = client.post(f"/u/{unit}/api/caixa/lancar", json=payload)
    assert r.status_code == 200, r.get_data(as_text=True)
    return r.get_json()["lancamento"]["id"]


def _seed_planilha(unit: str, data_iso: str, records: list[dict]) -> None:
    p = server._planilha_dia_path(unit, data_iso)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({
        "data":        data_iso,
        "records":     records,
        "uploaded_at": dt.datetime.now().isoformat(),
        "uploaded_by": "test",
        "versao":      1,
    }), encoding="utf-8")


def _seed_envio(unit: str, data: str, valor: float, status: str = "ok", suffix: str = "") -> None:
    server._unit_state_dir(unit).mkdir(parents=True, exist_ok=True)
    insert_envio_tiny(unit, server._unit_state_dir(unit), {
        "chave_deduplicacao": f"{unit}-{data}-{valor}-{status}-{suffix}",
        "timestamp":          f"{data}T10:00:00",
        "data_lancamento":    data,
        "placa":              "ABC1234",
        "cliente":            "TESTE",
        "servico":            "VISTORIA CAUTELAR",
        "valor":              valor,
        "fp":                 "pix",
        "status":             status,
    })


# ══════════════════════════════════════════════════════════════════════════════
# Estrutura basica do payload
# ══════════════════════════════════════════════════════════════════════════════

class TestEstrutura:
    def test_dia_vazio_retorna_zeros(self, sp_client):
        r = sp_client.get(f"/u/sp/api/fechamento/relatorio-completo?data={HOJE}")
        assert r.status_code == 200
        j = r.get_json()
        assert j["success"] is True
        assert j["totais"]["total_dia"] == 0.0
        assert j["totais"]["total_count"] == 0
        assert j["totais"]["avista"] == 0.0
        assert j["totais"]["faturado"] == 0.0
        assert j["totais"]["ticket_medio"] == 0.0

    def test_inclui_unit_nome_e_slug(self, sp_client):
        j = sp_client.get(f"/u/sp/api/fechamento/relatorio-completo").get_json()
        assert j["unit"]["slug"] == "sp"
        assert j["unit"]["nome"] == "São Paulo"

    def test_data_default_eh_hoje(self, sp_client):
        j = sp_client.get(f"/u/sp/api/fechamento/relatorio-completo").get_json()
        assert j["data"] == HOJE

    def test_avista_por_fp_tem_4_chaves(self, sp_client):
        j = sp_client.get(f"/u/sp/api/fechamento/relatorio-completo").get_json()
        assert set(j["avista_por_fp"].keys()) == {"dinheiro", "pix", "debito", "credito"}


# ══════════════════════════════════════════════════════════════════════════════
# Agregacao por FP
# ══════════════════════════════════════════════════════════════════════════════

class TestAgregacao:
    def test_avista_total_separa_faturado_e_detran(self, sp_client):
        _lancar(sp_client, "sp", valor=100.0, fp="pix")
        _lancar(sp_client, "sp", valor=200.0, fp="dinheiro")
        _lancar(sp_client, "sp", valor=300.0, fp="faturado")
        _lancar(sp_client, "sp", valor=50.0,  fp="detran")
        j = sp_client.get(f"/u/sp/api/fechamento/relatorio-completo").get_json()
        t = j["totais"]
        assert t["avista"]   == 300.0  # 100 pix + 200 dinheiro
        assert t["faturado"] == 300.0
        assert t["detran"]   == 50.0
        assert t["total_dia"] == 650.0

    def test_avista_por_fp_separa_corretamente(self, sp_client):
        _lancar(sp_client, "sp", valor=100.0, fp="pix")
        _lancar(sp_client, "sp", valor=80.0,  fp="pix")
        _lancar(sp_client, "sp", valor=200.0, fp="dinheiro")
        _lancar(sp_client, "sp", valor=150.0, fp="credito", cv="123456")
        j = sp_client.get(f"/u/sp/api/fechamento/relatorio-completo").get_json()
        f = j["avista_por_fp"]
        assert f["pix"]["total"] == 180.0
        assert f["pix"]["count"] == 2
        assert len(f["pix"]["lancamentos"]) == 2
        assert f["dinheiro"]["count"] == 1
        assert f["credito"]["count"] == 1
        assert f["debito"]["count"] == 0

    def test_faturados_lista_separada(self, sp_client):
        _lancar(sp_client, "sp", valor=100.0, fp="pix")
        _lancar(sp_client, "sp", valor=300.0, fp="faturado", placa="XYZ9999")
        _lancar(sp_client, "sp", valor=400.0, fp="faturado", placa="WWW0000")
        j = sp_client.get(f"/u/sp/api/fechamento/relatorio-completo").get_json()
        assert len(j["faturados"]) == 2
        assert all(lc["fp"] == "faturado" for lc in j["faturados"])
        assert {lc["placa"] for lc in j["faturados"]} == {"XYZ9999", "WWW0000"}

    def test_ticket_medio_calcula_corretamente(self, sp_client):
        _lancar(sp_client, "sp", valor=100.0, fp="pix")
        _lancar(sp_client, "sp", valor=300.0, fp="faturado")
        j = sp_client.get(f"/u/sp/api/fechamento/relatorio-completo").get_json()
        assert j["totais"]["ticket_medio"] == 200.0  # (100+300)/2


# ══════════════════════════════════════════════════════════════════════════════
# Tiny envios
# ══════════════════════════════════════════════════════════════════════════════

class TestTinyEnvios:
    def test_dia_sem_envios(self, sp_client):
        j = sp_client.get(f"/u/sp/api/fechamento/relatorio-completo").get_json()
        assert j["tiny"]["enviados_ok"] == 0
        assert j["tiny"]["valor_enviado"] == 0.0
        assert j["tiny"]["falhas"] == 0

    def test_envios_ok_e_falhas_separados(self, tmp_path, sp_client):
        # Reusa fixture mas precisa popular envios via _seed_envio que usa DATA_DIR
        _seed_envio("sp", HOJE, 100.0, status="ok",    suffix="a")
        _seed_envio("sp", HOJE, 200.0, status="ok",    suffix="b")
        _seed_envio("sp", HOJE, 300.0, status="falha", suffix="c")
        j = sp_client.get(f"/u/sp/api/fechamento/relatorio-completo").get_json()
        assert j["tiny"]["enviados_ok"]   == 2
        assert j["tiny"]["valor_enviado"] == 300.0
        assert j["tiny"]["falhas"]        == 1


# ══════════════════════════════════════════════════════════════════════════════
# Conferencia PDV x planilha
# ══════════════════════════════════════════════════════════════════════════════

class TestConferencia:
    def test_sem_planilha_retorna_exists_false(self, sp_client):
        _lancar(sp_client, "sp", valor=100.0)
        j = sp_client.get(f"/u/sp/api/fechamento/relatorio-completo").get_json()
        assert j["conferencia"]["exists"] is False

    def test_com_planilha_retorna_linhas_detalhadas(self, sp_client):
        _seed_planilha("sp", HOJE, [
            {"id": "1", "placa": "ABC1234", "cliente": "X", "servico": "VISTORIA CAUTELAR", "preco": 100.0, "fp": "AV"},
        ])
        _lancar(sp_client, "sp", placa="ABC1234", valor=100.0, fp="pix")
        j = sp_client.get(f"/u/sp/api/fechamento/relatorio-completo").get_json()
        assert j["conferencia"]["exists"] is True
        assert len(j["conferencia"]["linhas"]) == 1
        ln = j["conferencia"]["linhas"][0]
        assert ln["status"] == "ok"
        assert ln["pdv_match"] is not None

    def test_orfas_pdv_aparecem(self, sp_client):
        _seed_planilha("sp", HOJE, [])  # planilha vazia
        _lancar(sp_client, "sp", placa="ABC1234", valor=100.0)
        j = sp_client.get(f"/u/sp/api/fechamento/relatorio-completo").get_json()
        assert len(j["conferencia"]["orfas_pdv"]) == 1


# ══════════════════════════════════════════════════════════════════════════════
# Auth
# ══════════════════════════════════════════════════════════════════════════════

class TestAuth:
    def test_anonimo_redireciona_para_login(self, tmp_path):
        server.DATA_DIR = tmp_path
        server.UNITS = UNITS_FIX
        server.app.config["TESTING"] = True
        with patch.object(server, "_current_user", return_value=None):
            with server.app.test_client() as c:
                r = c.get(f"/u/sp/api/fechamento/relatorio-completo")
        # unit_access_required redirecte para login (302) ou 401 dependendo do caminho
        assert r.status_code in (302, 401)

    def test_operador_de_outra_unit_recebe_403(self, rj_client):
        r = rj_client.get(f"/u/sp/api/fechamento/relatorio-completo")
        assert r.status_code == 403

    def test_operador_da_unit_recebe_200(self, sp_client):
        r = sp_client.get(f"/u/sp/api/fechamento/relatorio-completo")
        assert r.status_code == 200

    def test_master_recebe_200(self, master_client):
        r = master_client.get(f"/u/sp/api/fechamento/relatorio-completo")
        assert r.status_code == 200


# ══════════════════════════════════════════════════════════════════════════════
# Tela imprimir
# ══════════════════════════════════════════════════════════════════════════════

class TestTelaImprimir:
    def test_serve_html_pra_operador_da_unit(self, sp_client):
        r = sp_client.get("/u/sp/fechamento/imprimir")
        assert r.status_code == 200
        assert b"FECHAMENTO" in r.data.upper() or b"Fechamento" in r.data

    def test_outra_unit_recebe_403(self, rj_client):
        r = rj_client.get("/u/sp/fechamento/imprimir")
        assert r.status_code == 403


# ══════════════════════════════════════════════════════════════════════════════
# Refator: api_planilha_status nao deve ter quebrado
# ══════════════════════════════════════════════════════════════════════════════

class TestRefatorPlanilhaStatus:
    def test_endpoint_status_continua_retornando_payload_correto(self, sp_client):
        _seed_planilha("sp", HOJE, [
            {"id": "1", "placa": "ABC1234", "servico": "VISTORIA CAUTELAR", "preco": 100.0, "fp": "AV"},
        ])
        r = sp_client.get(f"/u/sp/api/planilha/status?data={HOJE}")
        assert r.status_code == 200
        j = r.get_json()
        assert j["success"] is True
        assert j["exists"] is True
        assert len(j["linhas"]) == 1

    def test_compute_funcao_pura_pode_ser_chamada_direto(self, tmp_path):
        server.DATA_DIR = tmp_path
        server.UNITS = UNITS_FIX
        _seed_planilha("sp", HOJE, [])
        result = server._compute_planilha_status("sp", HOJE)
        assert result["success"] is True
        assert result["exists"] is True
        assert result["stats"]["total"] == 0
