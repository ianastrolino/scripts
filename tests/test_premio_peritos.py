"""
Testes da Fase 3 — calculo de premio por perito.

Regra (definida com Ian em 2026-05-13):
- Meta: 171 vistorias no mes corrente
- Ao bater, TODAS as vistorias do mes pagam (retroativo)
- Valores:
    CAUTELAR + PINTURA → R$ 10
    CAUTELAR (sem pintura) → R$ 5
    TRANSFERENCIA / VISTORIA MOVEL → R$ 2
    Outros (verificacao, etc) → R$ 0 (mas contam pra meta)
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
from caixa_db import upsert_vistorias_planilha  # noqa: E402


UNITS_FIX = {"sp": {"nome": "São Paulo"}}
MASTER_USER = {"email": "m@a.com", "name": "M", "unit": None, "master": True}


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


# ══════════════════════════════════════════════════════════════════════════════
# _classifica_servico_premio
# ══════════════════════════════════════════════════════════════════════════════

class TestClassifica:
    @pytest.mark.parametrize("servico,categoria", [
        ("CAUTELAR + PINTURA",           "cautelar_pintura"),
        ("VISTORIA CAUTELAR + PINTURA",  "cautelar_pintura"),
        ("CAUTELAR PINTURA",             "cautelar_pintura"),
        ("VISTORIA CAUTELAR",            "cautelar"),
        ("LAUDO CAUTELAR",               "cautelar"),
        ("CAUTELAR PESADA",              "cautelar"),
        ("LAUDO DE TRANSFERENCIA",       "transferencia"),
        ("VISTORIA DE TRANSFERENCIA",    "transferencia"),
        ("VISTORIA MOVEL",               "transferencia"),
        ("VERIFICACAO VEICULAR",         "outros"),
        ("PESQUISA AVULSA",              "outros"),
        ("BAIXA PERMANENTE",             "outros"),
        ("",                             "outros"),
    ])
    def test_match_categoria(self, servico, categoria):
        assert server._classifica_servico_premio(servico) == categoria

    def test_pintura_tem_prioridade_sobre_cautelar(self):
        """Servico com 'PINTURA' e 'CAUTELAR' eh cautelar_pintura, nao cautelar."""
        assert server._classifica_servico_premio("CAUTELAR + PINTURA") == "cautelar_pintura"


# ══════════════════════════════════════════════════════════════════════════════
# _calcula_premio_perito
# ══════════════════════════════════════════════════════════════════════════════

class TestCalculaPremio:
    def test_abaixo_meta_premio_zero(self):
        r = server._calcula_premio_perito(170, {
            "cautelar_pintura": 50, "cautelar": 50, "transferencia": 70,
        })
        assert r["bate_meta"] is False
        assert r["premio_total"] == 0.0
        assert r["qtd_total"] == 170

    def test_exatamente_171_bate_meta(self):
        r = server._calcula_premio_perito(171, {
            "cautelar_pintura": 0, "cautelar": 0, "transferencia": 171,
        })
        assert r["bate_meta"] is True
        # 171 transferencias × R$ 2 = R$ 342
        assert r["premio_total"] == 342.0

    def test_171_cautelar_pintura(self):
        r = server._calcula_premio_perito(171, {
            "cautelar_pintura": 171, "cautelar": 0, "transferencia": 0,
        })
        # 171 × R$ 10 = R$ 1710
        assert r["premio_total"] == 1710.0

    def test_mistura_de_tipos(self):
        r = server._calcula_premio_perito(200, {
            "cautelar_pintura": 50,  # 50 × 10 = 500
            "cautelar":         100, # 100 × 5 = 500
            "transferencia":    40,  # 40 × 2 = 80
            "outros":           10,  # 10 × 0 = 0
        })
        assert r["bate_meta"] is True
        assert r["premio_total"] == 1080.0

    def test_outros_conta_pra_meta_mas_paga_zero(self):
        """160 cautelar + 15 verificacao = 175 (bate meta), mas verificacao paga 0."""
        r = server._calcula_premio_perito(175, {
            "cautelar_pintura": 0, "cautelar": 160,
            "transferencia": 0, "outros": 15,
        })
        assert r["bate_meta"] is True
        # Verificacao paga 0; so cautelar entra (160 × 5 = 800)
        assert r["premio_total"] == 800.0

    def test_breakdown_por_categoria(self):
        r = server._calcula_premio_perito(200, {
            "cautelar_pintura": 50, "cautelar": 100, "transferencia": 50,
        })
        cats = r["premio_por_categoria"]
        assert cats["cautelar_pintura"]["subtotal"] == 500.0
        assert cats["cautelar"]["subtotal"]         == 500.0
        assert cats["transferencia"]["subtotal"]    == 100.0
        # Valor unit fixo
        assert cats["cautelar_pintura"]["valor_unit"] == 10.0
        assert cats["cautelar"]["valor_unit"]         == 5.0
        assert cats["transferencia"]["valor_unit"]    == 2.0

    def test_breakdown_zero_quando_nao_bate(self):
        r = server._calcula_premio_perito(170, {
            "cautelar_pintura": 50, "cautelar": 50, "transferencia": 70,
        })
        # Mesmo com vistorias, subtotal eh 0 porque nao bateu meta
        for cat, b in r["premio_por_categoria"].items():
            assert b["subtotal"] == 0.0


# ══════════════════════════════════════════════════════════════════════════════
# Endpoint /api/relatorio/vistoriadores inclui premio
# ══════════════════════════════════════════════════════════════════════════════

def _v(**kw):
    base = {
        "data": "2026-05-13", "placa": "ABC1234", "cliente": "X",
        "servico": "VISTORIA CAUTELAR", "valor": 100.0, "fp": "AV",
        "perito": "JOAO", "arquivo": "p.xls",
    }
    base.update(kw)
    return base


class TestEndpointPremio:
    def test_perito_abaixo_meta_premio_zero(self, master_client, setup):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        # 10 vistorias — bem abaixo da meta
        vistorias = [
            _v(placa=f"A{i:04d}", perito="JOAO", servico="VISTORIA CAUTELAR")
            for i in range(10)
        ]
        upsert_vistorias_planilha("sp", unit_dir, vistorias)
        r = master_client.get("/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31&unit=sp")
        body = r.get_json()
        joao = next(p for p in body["peritos"] if p["perito"] == "JOAO")
        assert joao["premio"]["bate_meta"] is False
        assert joao["premio"]["premio_total"] == 0.0
        assert joao["premio"]["qtd_total"] == 10
        assert joao["premio"]["meta"] == 171

    def test_perito_bate_meta_calcula_premio(self, master_client, setup):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        # 171 cautelares → 171 × R$ 5 = R$ 855
        vistorias = [
            _v(placa=f"B{i:04d}", perito="MARIA", servico="VISTORIA CAUTELAR")
            for i in range(171)
        ]
        upsert_vistorias_planilha("sp", unit_dir, vistorias)
        r = master_client.get("/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31&unit=sp")
        body = r.get_json()
        maria = next(p for p in body["peritos"] if p["perito"] == "MARIA")
        assert maria["premio"]["bate_meta"] is True
        assert maria["premio"]["premio_total"] == 855.0

    def test_totais_inclui_premio_consolidado(self, master_client, setup):
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        # JOAO bate (171 cautelar = R$ 855); PEDRO nao (50 cautelar = 0)
        vistorias = []
        for i in range(171):
            vistorias.append(_v(placa=f"J{i:04d}", perito="JOAO", servico="VISTORIA CAUTELAR"))
        for i in range(50):
            vistorias.append(_v(placa=f"P{i:04d}", perito="PEDRO", servico="VISTORIA CAUTELAR"))
        upsert_vistorias_planilha("sp", unit_dir, vistorias)
        r = master_client.get("/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31&unit=sp")
        body = r.get_json()
        assert body["totais"]["premio_total"] == 855.0
        assert body["totais"]["peritos_batendo"] == 1

    def test_premio_regra_no_payload(self, master_client, setup):
        r = master_client.get("/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31")
        body = r.get_json()
        assert body["premio_regra"]["meta"] == 171
        assert body["premio_regra"]["valores"]["cautelar_pintura"] == 10.0
        assert body["premio_regra"]["valores"]["cautelar"]         == 5.0
        assert body["premio_regra"]["valores"]["transferencia"]    == 2.0

    def test_premio_breakdown_por_servico(self, master_client, setup):
        """Confere que servicos diferentes entram em categorias corretas."""
        unit_dir = server._unit_state_dir("sp")
        unit_dir.mkdir(parents=True, exist_ok=True)
        vistorias = []
        # 50 cautelar+pintura, 60 cautelar, 61 transferencia = 171 total
        for i in range(50):
            vistorias.append(_v(placa=f"CP{i:04d}", perito="ANA", servico="CAUTELAR + PINTURA"))
        for i in range(60):
            vistorias.append(_v(placa=f"CA{i:04d}", perito="ANA", servico="VISTORIA CAUTELAR"))
        for i in range(61):
            vistorias.append(_v(placa=f"TR{i:04d}", perito="ANA", servico="LAUDO DE TRANSFERENCIA"))
        upsert_vistorias_planilha("sp", unit_dir, vistorias)
        r = master_client.get("/api/relatorio/vistoriadores?inicio=2026-05-01&fim=2026-05-31&unit=sp")
        ana = next(p for p in r.get_json()["peritos"] if p["perito"] == "ANA")
        cats = ana["premio"]["premio_por_categoria"]
        # 50 × 10 = 500; 60 × 5 = 300; 61 × 2 = 122; total 922
        assert cats["cautelar_pintura"]["subtotal"] == 500.0
        assert cats["cautelar"]["subtotal"]         == 300.0
        assert cats["transferencia"]["subtotal"]    == 122.0
        assert ana["premio"]["premio_total"]        == 922.0
