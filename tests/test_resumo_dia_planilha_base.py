"""
Testes do novo _resumo_dia_unit com planilha como fonte de verdade.

Modelo (Ian 2026-05-15):
- Planilha eh fonte do VOLUME real (AV + FA)
- PDV registra AV pagos no caixa
- Avulsos = PDV cujo (placa, categoria) NAO bate com planilha
- Vistorias = planilha + avulsos

Bug original: Moema mostrava 69 vistorias quando real eram 45 + 2 avulsos.
Causa: logica antiga somava todo PDV nao-pareado-com-Tiny como vistoria
adicional, inflando o numero.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
from pathlib import Path

import pytest

os.environ.setdefault("SECRET_KEY", "test-secret-key-pytest")
os.environ.setdefault("USERS_CONFIG", "{}")
os.environ.setdefault("UNITS_CONFIG", "{}")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import server  # noqa: E402
from caixa_db import (  # noqa: E402
    insert_envio_tiny, upsert_vistorias_planilha, insert_lancamento,
)


UNITS_FIX = {"sp": {"nome": "São Paulo"}}
HOJE = dt.date.today().isoformat()


@pytest.fixture
def setup(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    return tmp_path


@pytest.fixture
def unit_dir(setup):
    d = server._unit_state_dir("sp")
    d.mkdir(parents=True, exist_ok=True)
    return d


# ══════════════════════════════════════════════════════════════════════════════
# _categoria_pra_dedup
# ══════════════════════════════════════════════════════════════════════════════

class TestCategoria:
    @pytest.mark.parametrize("servico,esperado", [
        ("VISTORIA CAUTELAR",        "cautelar"),
        ("CAUTELAR + PINTURA",       "cautelar"),
        ("CAUTELAR PESADA",          "cautelar"),
        ("LAUDO CAUTELAR",           "cautelar"),
        ("LAUDO DE TRANSFERENCIA",   "transferencia"),
        ("LAUDO TRANSFERENCIA",      "transferencia"),
        ("VISTORIA MOVEL",           "transferencia"),
        ("VISTORIA DE TRANSFERÊNCIA","transferencia"),
        ("CONSULTA DE GRAVAME",      "outros:CONSULTA DE GRAVAME"),
        ("BAIXA PERMANENTE",         "outros:BAIXA PERMANENTE"),
        ("PESQUISA AVULSA",          "outros:PESQUISA AVULSA"),
        ("",                         "outros:"),
    ])
    def test_classificacao(self, servico, esperado):
        assert server._categoria_pra_dedup(servico) == esperado


def _vist(**kw):
    base = {"data": HOJE, "placa": "ABC1234", "cliente": "X",
            "servico": "VISTORIA CAUTELAR", "valor": 100.0, "fp": "AV",
            "perito": "JOAO"}
    base.update(kw)
    return base


def _lc(**kw):
    base = {"id": "lc-1", "unit": "sp", "data": HOJE, "hora": "10:00",
            "timestamp": f"{HOJE}T10:00:00", "placa": "ABC1234", "cliente": "X",
            "servico": "VISTORIA CAUTELAR", "valor": 100.0, "fp": "pix"}
    base.update(kw)
    return base


def _envio(**kw):
    base = {"chave_deduplicacao": "k-1", "timestamp": f"{HOJE}T10:00:00",
            "data_lancamento": HOJE, "placa": "ABC1234", "cliente": "X",
            "servico": "VISTORIA CAUTELAR", "valor": 100.0, "fp": "AV",
            "status": "enviado"}
    base.update(kw)
    return base


# ══════════════════════════════════════════════════════════════════════════════
# Sem planilha → fallback pra logica antiga
# ══════════════════════════════════════════════════════════════════════════════

class TestFallbackSemPlanilha:
    def test_sem_planilha_so_envios(self, unit_dir):
        insert_envio_tiny("sp", unit_dir, _envio())
        r = server._resumo_dia_unit("sp", unit_dir, HOJE)
        assert r["vistorias"] == 1
        assert r["total"] == 100.0

    def test_sem_planilha_pdv_e_envio_distintos_somam(self, unit_dir):
        """Bug antigo: PDV nao-pareado soma como vistoria. Mantido no fallback."""
        insert_envio_tiny("sp", unit_dir, _envio(placa="AAA1111"))
        insert_lancamento(unit_dir, _lc(id="lc-x", placa="BBB2222"))
        r = server._resumo_dia_unit("sp", unit_dir, HOJE)
        assert r["vistorias"] == 2


# ══════════════════════════════════════════════════════════════════════════════
# Com planilha → nova logica
# ══════════════════════════════════════════════════════════════════════════════

class TestComPlanilha:
    def test_caso_simples_so_planilha(self, unit_dir):
        upsert_vistorias_planilha("sp", unit_dir, [_vist()])
        r = server._resumo_dia_unit("sp", unit_dir, HOJE)
        assert r["vistorias"] == 1

    def test_pdv_pareia_por_categoria_nao_soma(self, unit_dir):
        """Bug Moema: lancamento PDV pra cobrar AV NAO deve somar como nova
        vistoria — o serviço ja esta na planilha."""
        upsert_vistorias_planilha("sp", unit_dir, [
            _vist(placa="ABC1234", servico="VISTORIA CAUTELAR"),
        ])
        # PDV: mesmo carro, servico ligeiramente diferente (CAUTELAR PESADA)
        insert_lancamento(unit_dir, _lc(
            id="lc-1", placa="ABC1234", servico="CAUTELAR PESADA", fp="pix",
        ))
        r = server._resumo_dia_unit("sp", unit_dir, HOJE)
        # Planilha: 1, PDV pareia (mesmo placa+categoria=cautelar) → vistorias = 1
        assert r["vistorias"] == 1

    def test_cautelar_pintura_pareia_com_cautelar_pdv(self, unit_dir):
        """Upsell: planilha CAUTELAR + PINTURA pareia com PDV CAUTELAR."""
        upsert_vistorias_planilha("sp", unit_dir, [
            _vist(placa="XYZ9999", servico="CAUTELAR + PINTURA", valor=400),
        ])
        insert_lancamento(unit_dir, _lc(
            id="lc-cp", placa="XYZ9999", servico="VISTORIA CAUTELAR", valor=400,
        ))
        r = server._resumo_dia_unit("sp", unit_dir, HOJE)
        assert r["vistorias"] == 1

    def test_avulso_pdv_sem_planilha_soma(self, unit_dir):
        """PDV com placa que NAO esta na planilha = avulso = +1 vistoria."""
        upsert_vistorias_planilha("sp", unit_dir, [
            _vist(placa="ABC1234"),
        ])
        # PDV: outra placa, outro servico
        insert_lancamento(unit_dir, _lc(
            id="lc-gv", placa="ZZZ0000",
            servico="CONSULTA DE GRAVAME", valor=10,
        ))
        r = server._resumo_dia_unit("sp", unit_dir, HOJE)
        # Planilha 1 + 1 avulso = 2
        assert r["vistorias"] == 2

    def test_multiplos_servicos_mesmo_carro(self, unit_dir):
        """Mesmo carro, dois serviços diferentes (cautelar + transferencia) —
        ambos contam, PDV pareia individualmente por categoria."""
        upsert_vistorias_planilha("sp", unit_dir, [
            _vist(placa="GEK6319", servico="VISTORIA CAUTELAR",      valor=350),
            _vist(placa="GEK6319", servico="LAUDO DE TRANSFERENCIA", valor=150),
        ])
        # PDV cobrou os 2 — cautelar com nome ligeiramente diferente,
        # transferencia mantendo "TRANSFERENCIA" no nome
        insert_lancamento(unit_dir, _lc(
            id="lc-c", placa="GEK6319", servico="CAUTELAR PESADA", valor=350,
        ))
        insert_lancamento(unit_dir, _lc(
            id="lc-t", placa="GEK6319", servico="LAUDO DE TRANSFERENCIA COM VISTORIA", valor=150,
        ))
        r = server._resumo_dia_unit("sp", unit_dir, HOJE)
        assert r["vistorias"] == 2  # planilha 2, PDV pareia ambos
        assert r["lancamentos_pdv"] == 2

    def test_caso_moema_real(self, unit_dir):
        """Reproduz caso real: 45 vistorias planilha + 22 PDV (pagamentos)
        + 2 avulsos no Tiny não-planilha. Esperado: 45 + 0 do PDV (todos
        pareiam) = 45. Tiny não infla (vem por planilha pre-importada)."""
        # 45 cautelares na planilha
        upsert_vistorias_planilha("sp", unit_dir, [
            _vist(placa=f"P{i:04d}", servico="VISTORIA CAUTELAR")
            for i in range(45)
        ])
        # 22 PDV pra cobrar dessas (cada um eh pagamento de 1 cautelar)
        for i in range(22):
            insert_lancamento(unit_dir, _lc(
                id=f"lc-{i}", placa=f"P{i:04d}", servico="CAUTELAR PESADA",
            ))
        r = server._resumo_dia_unit("sp", unit_dir, HOJE)
        # 45 planilha + 0 avulsos = 45
        assert r["vistorias"] == 45
        assert r["lancamentos_pdv"] == 22

    def test_pdv_duplicado_no_mesmo_avulso_conta_1(self, unit_dir):
        """Operadora lancou 2x o mesmo avulso por engano — deve contar 1."""
        upsert_vistorias_planilha("sp", unit_dir, [
            _vist(placa="ABC1234"),
        ])
        # 2 PDV pra mesma placa+servico avulso (não está na planilha)
        insert_lancamento(unit_dir, _lc(
            id="lc-1", placa="ZZZ0000", servico="CONSULTA DE GRAVAME", valor=10,
        ))
        insert_lancamento(unit_dir, _lc(
            id="lc-2", placa="ZZZ0000", servico="CONSULTA DE GRAVAME", valor=10,
        ))
        r = server._resumo_dia_unit("sp", unit_dir, HOJE)
        # 1 planilha + 1 avulso (dedupado) = 2
        assert r["vistorias"] == 2

    def test_total_vem_dos_envios_tiny(self, unit_dir):
        """Total financeiro continua vindo dos envios_erp, nao da planilha."""
        upsert_vistorias_planilha("sp", unit_dir, [_vist()])
        insert_envio_tiny("sp", unit_dir, _envio(valor=100, fp="FA"))
        r = server._resumo_dia_unit("sp", unit_dir, HOJE)
        assert r["fa"] == 100.0
        assert r["av"] == 0
        assert r["total"] == 100.0
