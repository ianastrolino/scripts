"""
Testes do enriquecimento de historico Tiny: marca/modelo + servico.

Cobre:
- build_history inclui servico (mudanca de 2026-04-30)
- _build_planilha_modelo_index parseia planilha-dia corretamente
- _enrich_record_modelo so popula quando rec.modelo eh vazio
- Lookup eh case-insensitive e ignora hifen/espacos na placa
- Planilha inexistente / corrompida nao quebra
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path

import pytest

os.environ.setdefault("SECRET_KEY", "test-secret-key-pytest")
os.environ.setdefault("USERS_CONFIG", "{}")
os.environ.setdefault("UNITS_CONFIG", "{}")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import server  # noqa: E402
from tiny_import import NormalizedRecord, build_history  # noqa: E402


def _make_rec(**kw) -> NormalizedRecord:
    defaults = dict(
        data="2026-04-30", modelo="", placa="", cliente="X",
        servico="", fp="pix", preco="100",
        origem_arquivo="t", linha_origem=0,
        chave_deduplicacao="x", av_pagamento="", cpf="",
    )
    defaults.update(kw)
    return NormalizedRecord(**defaults)


# ══════════════════════════════════════════════════════════════════════════════
# build_history
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildHistory:
    def test_inclui_placa(self):
        rec = _make_rec(placa="ABC1234")
        assert "Placa ABC1234" in build_history(rec)

    def test_inclui_modelo_quando_presente(self):
        rec = _make_rec(placa="ABC1234", modelo="FORD KA")
        h = build_history(rec)
        assert "Placa ABC1234" in h
        assert "FORD KA" in h

    def test_inclui_servico_quando_presente(self):
        rec = _make_rec(placa="ABC1234", servico="VISTORIA CAUTELAR")
        h = build_history(rec)
        assert "Placa ABC1234" in h
        assert "VISTORIA CAUTELAR" in h

    def test_omite_modelo_vazio(self):
        rec = _make_rec(placa="ABC1234", modelo="", servico="CAUTELAR")
        h = build_history(rec)
        # Sem modelo, mas com servico
        assert "Placa ABC1234" in h
        assert "CAUTELAR" in h
        # Nao deve ter " |  | " (vazio entre separadores)
        assert " |  | " not in h

    def test_omite_servico_vazio(self):
        rec = _make_rec(placa="ABC1234", modelo="FORD KA")
        h = build_history(rec)
        assert "Placa ABC1234" in h
        assert "FORD KA" in h

    def test_inclui_cpf_formatado(self):
        rec = _make_rec(placa="ABC1234", cpf="12345678901")
        h = build_history(rec)
        assert "CPF 123.456.789-01" in h

    def test_inclui_cnpj_formatado(self):
        rec = _make_rec(placa="ABC1234", cpf="12345678901234")
        h = build_history(rec)
        assert "CNPJ 12.345.678/9012-34" in h

    def test_ordem_placa_modelo_servico(self):
        rec = _make_rec(placa="ABC1234", modelo="FORD KA", servico="VISTORIA CAUTELAR")
        h = build_history(rec)
        # A ordem deve ser: Placa, Modelo, Servico
        i_placa = h.index("Placa ABC1234")
        i_modelo = h.index("FORD KA")
        i_servico = h.index("VISTORIA CAUTELAR")
        assert i_placa < i_modelo < i_servico

    def test_truncado_em_250_chars(self):
        rec = _make_rec(placa="A" * 100, modelo="B" * 100, servico="C" * 100)
        h = build_history(rec)
        assert len(h) <= 250


# ══════════════════════════════════════════════════════════════════════════════
# _build_planilha_modelo_index
# ══════════════════════════════════════════════════════════════════════════════

class TestPlanilhaModeloIndex:
    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path):
        server.DATA_DIR = tmp_path
        server.UNITS = {"sp": {"nome": "SP"}}

    def _seed(self, unit: str, data: str, records: list[dict]) -> None:
        p = server._planilha_dia_path(unit, data)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({"records": records}), encoding="utf-8")

    def test_planilha_inexistente_retorna_dict_vazio(self):
        idx = server._build_planilha_modelo_index("sp", "2026-04-30")
        assert idx == {}

    def test_indexa_placa_normalizada(self):
        self._seed("sp", "2026-04-30", [
            {"placa": "ABC-1234", "modelo": "FORD KA"},
        ])
        idx = server._build_planilha_modelo_index("sp", "2026-04-30")
        # Hifen removido
        assert idx == {"ABC1234": "FORD KA"}

    def test_modelo_vai_pra_uppercase(self):
        self._seed("sp", "2026-04-30", [{"placa": "ABC1234", "modelo": "ford ka"}])
        idx = server._build_planilha_modelo_index("sp", "2026-04-30")
        assert idx["ABC1234"] == "FORD KA"

    def test_ignora_records_sem_modelo(self):
        self._seed("sp", "2026-04-30", [
            {"placa": "ABC1234", "modelo": ""},
            {"placa": "XYZ9999", "modelo": "VW GOL"},
        ])
        idx = server._build_planilha_modelo_index("sp", "2026-04-30")
        assert "ABC1234" not in idx
        assert idx["XYZ9999"] == "VW GOL"

    def test_primeira_ocorrencia_da_placa_ganha(self):
        self._seed("sp", "2026-04-30", [
            {"placa": "ABC1234", "modelo": "FORD KA"},
            {"placa": "ABC1234", "modelo": "VW GOL"},  # mesmo placa, modelo diferente
        ])
        idx = server._build_planilha_modelo_index("sp", "2026-04-30")
        assert idx["ABC1234"] == "FORD KA"

    def test_planilha_corrompida_retorna_dict_vazio(self):
        p = server._planilha_dia_path("sp", "2026-04-30")
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("{ json invalido", encoding="utf-8")
        idx = server._build_planilha_modelo_index("sp", "2026-04-30")
        assert idx == {}


# ══════════════════════════════════════════════════════════════════════════════
# _enrich_record_modelo
# ══════════════════════════════════════════════════════════════════════════════

class TestEnrichRecordModelo:
    def test_preenche_modelo_vazio_via_lookup(self):
        rec = _make_rec(placa="ABC1234", modelo="")
        idx = {"ABC1234": "FORD KA"}
        server._enrich_record_modelo(rec, idx)
        assert rec.modelo == "FORD KA"

    def test_nao_sobrescreve_modelo_existente(self):
        rec = _make_rec(placa="ABC1234", modelo="VW GOL")
        idx = {"ABC1234": "FORD KA"}
        server._enrich_record_modelo(rec, idx)
        assert rec.modelo == "VW GOL"  # mantem original

    def test_placa_com_hifen_normaliza_antes_de_buscar(self):
        rec = _make_rec(placa="ABC-1234", modelo="")
        idx = {"ABC1234": "FORD KA"}
        server._enrich_record_modelo(rec, idx)
        assert rec.modelo == "FORD KA"

    def test_placa_lowercase_normaliza_antes_de_buscar(self):
        rec = _make_rec(placa="abc1234", modelo="")
        idx = {"ABC1234": "FORD KA"}
        server._enrich_record_modelo(rec, idx)
        assert rec.modelo == "FORD KA"

    def test_placa_nao_encontrada_mantem_modelo_vazio(self):
        rec = _make_rec(placa="ZZZ9999", modelo="")
        idx = {"ABC1234": "FORD KA"}
        server._enrich_record_modelo(rec, idx)
        assert rec.modelo == ""

    def test_indice_vazio_mantem_modelo_vazio(self):
        rec = _make_rec(placa="ABC1234", modelo="")
        server._enrich_record_modelo(rec, {})
        assert rec.modelo == ""
