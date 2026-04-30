"""
caixa_helpers.py — Funções puras do domínio Caixa do Dia.

Módulo sem dependências de estado global (DATA_DIR, UNITS, Flask).
Importável em testes sem precisar inicializar o servidor.
"""
from __future__ import annotations

from typing import Any


FP_VALIDOS = frozenset({"dinheiro", "debito", "credito", "pix", "faturado", "detran"})


def calcular_totais(lancamentos: list[dict[str, Any]]) -> dict[str, Any]:
    """Soma lançamentos por forma de pagamento e retorna totais."""
    totals: dict[str, float] = {fp: 0.0 for fp in FP_VALIDOS}
    for lc in lancamentos:
        fp = lc.get("fp", "")
        if fp in totals:
            totals[fp] += float(lc.get("valor", 0))
    totals["total"] = sum(totals.values())
    totals["total_avista"] = totals["total"] - totals["faturado"] - totals["detran"]
    return totals


def validar_lancamento(data: dict[str, Any]) -> str | None:
    """Valida payload de lançamento. Retorna mensagem de erro ou None se válido."""
    if not data.get("placa", "").strip():
        return "Placa obrigatoria."
    if not data.get("cliente", "").strip():
        return "Cliente obrigatorio."
    if not data.get("servico", "").strip():
        return "Servico obrigatorio."
    try:
        valor = float(data.get("valor", 0))
    except (TypeError, ValueError):
        return "Valor invalido."
    if valor < 0:
        return "Valor nao pode ser negativo."
    if data.get("fp") not in FP_VALIDOS:
        return "Forma de pagamento invalida."
    return None
