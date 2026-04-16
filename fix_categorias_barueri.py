#!/usr/bin/env python3
"""Corrige a categoria de contas a receber no Tiny (Barueri) que foram
importadas com a categoria errada (CAUTELAR COM ANALISE / LAUDO DE VERIFICACAO)
quando deveriam ser VISTORIA CAUTELAR.

Uso:
    # Apenas lista o que seria corrigido (dry-run):
    python3 fix_categorias_barueri.py

    # Aplica a correcao de fato:
    python3 fix_categorias_barueri.py --execute

    # Filtrar por periodo (opcional):
    python3 fix_categorias_barueri.py --de 2026-01-01 --ate 2026-04-30 --execute
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# IDs de categoria no Tiny Barueri
CATEGORIA_ERRADA_ID = 897968384   # CAUTELAR COM ANALISE (era LAUDO DE VERIFICACAO DE PINTURA)
CATEGORIA_CORRETA_ID = 897968382  # VISTORIA CAUTELAR


def load_env(env_file: Path) -> None:
    """Carrega variaveis de um arquivo .env para os.environ."""
    if not env_file.exists():
        print(f"[ERRO] Arquivo nao encontrado: {env_file}", file=sys.stderr)
        sys.exit(1)
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def fetch_contas_com_categoria(client, categoria_id: int, data_de: str | None, data_ate: str | None) -> list[dict]:
    """Busca todas as contas a receber com a categoria informada.
    A API de listagem nao retorna categoria, entao busca o detalhe de cada registro.
    """
    todos: list[dict] = []
    offset = 0
    limit = 100
    print("  Listando registros...", flush=True)
    while True:
        params: dict = {"limit": limit, "offset": offset}
        if data_de:
            params["dataInicial"] = data_de
        if data_ate:
            params["dataFinal"] = data_ate
        resp = client.request("GET", "contas-receber", params=params)
        page = resp.get("itens", [])
        todos.extend(page)
        if len(page) < limit:
            break
        offset += limit
        time.sleep(0.2)

    print(f"  {len(todos)} registros encontrados no periodo. Verificando categorias...", flush=True)
    errados: list[dict] = []
    for i, item in enumerate(todos):
        if (i + 1) % 20 == 0:
            print(f"  Verificando {i + 1}/{len(todos)}...", flush=True)
        try:
            detalhe = client.request("GET", f"contas-receber/{item['id']}")
            cat = detalhe.get("categoria") or {}
            cat_id = cat.get("id") if isinstance(cat, dict) else None
            if cat_id == categoria_id:
                item["_categoria_id"] = cat_id
                errados.append(item)
        except Exception as exc:
            print(f"  [AVISO] Nao conseguiu verificar ID {item['id']}: {exc}")
        time.sleep(0.15)
    return errados


def main() -> int:
    parser = argparse.ArgumentParser(description="Corrige categorias erradas no Tiny Barueri.")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Aplica as alteracoes. Sem esta flag roda em modo dry-run (apenas lista).",
    )
    parser.add_argument(
        "--de",
        metavar="AAAA-MM-DD",
        help="Data inicial para filtrar lancamentos (opcional).",
    )
    parser.add_argument(
        "--ate",
        metavar="AAAA-MM-DD",
        help="Data final para filtrar lancamentos (opcional).",
    )
    parser.add_argument(
        "--env-file",
        default=str(BASE_DIR / ".env.barueri"),
        help="Arquivo .env a usar (padrao: .env.barueri).",
    )
    args = parser.parse_args()

    # Carrega .env antes de importar tiny_import (que le os.environ)
    load_env(Path(args.env_file))

    # Importa TinyClient e helpers do tiny_import
    sys.path.insert(0, str(BASE_DIR))
    from tiny_import import TinyClient, load_config, project_path, merge_config

    config = load_config(None)
    tiny_config = config["tiny"]
    state_dir = project_path(config, "state_dir")
    state_dir.mkdir(parents=True, exist_ok=True)

    client = TinyClient(tiny_config, state_dir)

    modo = "EXECUCAO REAL" if args.execute else "DRY-RUN (apenas leitura)"
    print(f"\n=== fix_categorias_barueri.py — {modo} ===")
    print(f"Categoria errada : {CATEGORIA_ERRADA_ID}")
    print(f"Categoria correta: {CATEGORIA_CORRETA_ID}")
    if args.de or args.ate:
        print(f"Periodo          : {args.de or '(inicio)'} ate {args.ate or '(hoje)'}")
    print()

    print("Buscando contas a receber com categoria errada...", flush=True)
    itens = fetch_contas_com_categoria(client, CATEGORIA_ERRADA_ID, args.de, args.ate)

    if not itens:
        print("Nenhum lancamento encontrado com a categoria errada. Nada a fazer.")
        return 0

    print(f"Encontrados {len(itens)} lancamento(s) para corrigir:\n")
    print(f"{'ID':<14} {'Data':<12} {'Contato':<30} {'Historico':<45} {'Valor':>10}")
    print("-" * 115)
    for item in itens:
        contato = (item.get("contato") or {}).get("nome", "")[:29]
        historico = (item.get("historico") or "")[:44]
        data = item.get("data", "")
        valor = item.get("valor", 0)
        print(f"{item['id']:<14} {data:<12} {contato:<30} {historico:<45} {valor:>10.2f}")

    print()

    if not args.execute:
        print("Modo DRY-RUN: nenhuma alteracao foi feita.")
        print("Rode com --execute para aplicar as correcoes.")
        return 0

    # Executa as atualizacoes
    ok = 0
    falhas = 0
    for item in itens:
        item_id = item["id"]
        historico = (item.get("historico") or "")[:44]
        try:
            client.request(
                "PUT",
                f"contas-receber/{item_id}",
                json_body={"categoria": {"id": CATEGORIA_CORRETA_ID}},
            )
            print(f"  [OK] {item_id} — {historico}")
            ok += 1
        except Exception as exc:
            print(f"  [ERRO] {item_id} — {historico} — {exc}")
            falhas += 1
        time.sleep(0.3)

    print(f"\nConcluido: {ok} corrigido(s), {falhas} erro(s).")
    return 0 if falhas == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
