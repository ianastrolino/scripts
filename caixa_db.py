"""
caixa_db.py — Camada SQLite para persistência do caixa do dia.

Substitui caixa_dia.json. Uso futuro: basta trocar _load_caixa_dia e
_save_caixa_dia em server.py para usar estas funções.

SQLite é stdlib — zero novas dependências. O arquivo .db fica em
/data/{unit}/caixa_dia.db, no mesmo volume do Railway.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


_DDL = """
CREATE TABLE IF NOT EXISTS lancamentos (
    id        TEXT PRIMARY KEY,
    unit      TEXT NOT NULL,
    data      TEXT NOT NULL,
    hora      TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    placa     TEXT NOT NULL,
    cliente   TEXT NOT NULL,
    servico   TEXT NOT NULL,
    valor     REAL NOT NULL,
    fp        TEXT NOT NULL,
    cpf       TEXT NOT NULL DEFAULT ""
);
CREATE INDEX IF NOT EXISTS idx_lancamentos_unit_data ON lancamentos(unit, data);
"""

_MIGRATE_CPF = "ALTER TABLE lancamentos ADD COLUMN cpf TEXT NOT NULL DEFAULT \"\""

_DDL_DIV = """
CREATE TABLE IF NOT EXISTS divergencias (
    id        TEXT PRIMARY KEY,
    unit      TEXT NOT NULL,
    data      TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    placa     TEXT NOT NULL,
    cliente   TEXT NOT NULL DEFAULT "",
    servico   TEXT NOT NULL DEFAULT "",
    valor     REAL NOT NULL DEFAULT 0,
    fp        TEXT NOT NULL DEFAULT "",
    motivo    TEXT NOT NULL,
    pdv_valor REAL,
    pdv_fp    TEXT NOT NULL DEFAULT "",
    arquivo   TEXT NOT NULL DEFAULT ""
);
CREATE INDEX IF NOT EXISTS idx_div_unit_data ON divergencias(unit, data);
"""

_DDL_SNAPSHOT = """
CREATE TABLE IF NOT EXISTS planilhas_snapshot (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    unit        TEXT NOT NULL,
    data        TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    arquivos    TEXT NOT NULL DEFAULT "",
    records     TEXT NOT NULL,
    conferencia TEXT NOT NULL DEFAULT "{}",
    conferido   TEXT NOT NULL DEFAULT "[]",
    pdv_base    TEXT NOT NULL DEFAULT "null",
    origem      TEXT NOT NULL DEFAULT "import",
    autor       TEXT NOT NULL DEFAULT ""
);
CREATE INDEX IF NOT EXISTS idx_snap_unit_data ON planilhas_snapshot(unit, data);
CREATE INDEX IF NOT EXISTS idx_snap_unit_created ON planilhas_snapshot(unit, created_at);
"""


def _connect(unit_dir: Path) -> sqlite3.Connection:
    db_path = unit_dir / "caixa_dia.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_DDL)
    try:
        conn.execute(_MIGRATE_CPF)
        conn.commit()
    except sqlite3.OperationalError:
        pass  # column already exists
    conn.executescript(_DDL_DIV)
    conn.executescript(_DDL_SNAPSHOT)
    return conn


def migrate_from_json(unit: str, unit_dir: Path) -> int:
    """Migra caixa_dia.json para SQLite. Retorna número de registros migrados.
    Chame uma vez no startup se o .db não existir mas o .json existir.
    Lança exceção em falha real — o chamador decide o fallback.
    """
    json_path = unit_dir / "caixa_dia.json"
    db_path   = unit_dir / "caixa_dia.db"
    if db_path.exists() or not json_path.exists():
        return 0
    try:
        data = json.loads(json_path.read_text())
        lancamentos = data.get("lancamentos", [])
        if not lancamentos:
            return 0
        with _connect(unit_dir) as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO lancamentos "
                "(id, unit, data, hora, timestamp, placa, cliente, servico, valor, fp) "
                "VALUES (:id,:unit,:data,:hora,:timestamp,:placa,:cliente,:servico,:valor,:fp)",
                [
                    {**lc, "unit": unit, "data": lc.get("timestamp", "")[:10] or data.get("data", "")}
                    for lc in lancamentos
                ],
            )
        return len(lancamentos)
    except Exception:
        db_path.unlink(missing_ok=True)  # remove .db parcial para retry no próximo start
        raise


def load_lancamentos(unit: str, unit_dir: Path, today: str) -> list[dict[str, Any]]:
    with _connect(unit_dir) as conn:
        rows = conn.execute(
            "SELECT * FROM lancamentos WHERE unit=? AND data=? ORDER BY timestamp",
            (unit, today),
        ).fetchall()
    return [dict(r) for r in rows]


def insert_lancamento(unit_dir: Path, lancamento: dict[str, Any]) -> None:
    with _connect(unit_dir) as conn:
        conn.execute(
            "INSERT INTO lancamentos "
            "(id, unit, data, hora, timestamp, placa, cliente, servico, valor, fp, cpf) "
            "VALUES (:id,:unit,:data,:hora,:timestamp,:placa,:cliente,:servico,:valor,:fp,:cpf)",
            {**lancamento, "cpf": lancamento.get("cpf", "")},
        )


def update_lancamento(unit_dir: Path, lancamento_id: str, fields: dict[str, Any]) -> bool:
    """Atualiza campos de um lançamento. Retorna True se encontrado."""
    allowed = {"placa", "cliente", "servico", "valor", "fp", "cpf"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return False
    cols = ", ".join(f"{k}=?" for k in updates)
    vals = list(updates.values()) + [lancamento_id]
    with _connect(unit_dir) as conn:
        cur = conn.execute(f"UPDATE lancamentos SET {cols} WHERE id=?", vals)
        return cur.rowcount > 0


def insert_divergencia(unit_dir: Path, div: dict[str, Any]) -> None:
    with _connect(unit_dir) as conn:
        conn.execute(
            "INSERT INTO divergencias "
            "(id,unit,data,timestamp,placa,cliente,servico,valor,fp,motivo,pdv_valor,pdv_fp,arquivo) "
            "VALUES (:id,:unit,:data,:timestamp,:placa,:cliente,:servico,:valor,:fp,:motivo,:pdv_valor,:pdv_fp,:arquivo)",
            div,
        )


def load_divergencias_range(unit: str, unit_dir: Path, date_from: str, date_to: str) -> list[dict[str, Any]]:
    with _connect(unit_dir) as conn:
        rows = conn.execute(
            "SELECT * FROM divergencias WHERE unit=? AND data BETWEEN ? AND ? ORDER BY data, timestamp",
            (unit, date_from, date_to),
        ).fetchall()
    return [dict(r) for r in rows]


def load_lancamentos_range(unit: str, unit_dir: Path, date_from: str, date_to: str) -> list[dict[str, Any]]:
    """Retorna todos os lançamentos da unidade no intervalo [date_from, date_to] (ISO)."""
    with _connect(unit_dir) as conn:
        rows = conn.execute(
            "SELECT * FROM lancamentos WHERE unit=? AND data BETWEEN ? AND ? ORDER BY data, timestamp",
            (unit, date_from, date_to),
        ).fetchall()
    return [dict(r) for r in rows]


def delete_lancamento(unit_dir: Path, lancamento_id: str) -> bool:
    """Remove um lançamento. Retorna True se encontrado."""
    with _connect(unit_dir) as conn:
        cur = conn.execute("DELETE FROM lancamentos WHERE id=?", (lancamento_id,))
        return cur.rowcount > 0


# ── Snapshots de planilhas ────────────────────────────────────────────────────

def insert_snapshot(unit: str, unit_dir: Path, payload: dict[str, Any]) -> int:
    """Salva um snapshot do estado da conferência/fechamento. Retorna id gerado."""
    row = {
        "unit":        unit,
        "data":        payload.get("data", ""),
        "created_at":  payload.get("created_at", ""),
        "arquivos":    json.dumps(payload.get("arquivos", []), ensure_ascii=False),
        "records":     json.dumps(payload.get("records", []), ensure_ascii=False),
        "conferencia": json.dumps(payload.get("conferencia", {}), ensure_ascii=False),
        "conferido":   json.dumps(payload.get("conferido", []), ensure_ascii=False),
        "pdv_base":    json.dumps(payload.get("pdv_base", None), ensure_ascii=False),
        "origem":      payload.get("origem", "import"),
        "autor":       payload.get("autor", ""),
    }
    with _connect(unit_dir) as conn:
        cur = conn.execute(
            "INSERT INTO planilhas_snapshot "
            "(unit,data,created_at,arquivos,records,conferencia,conferido,pdv_base,origem,autor) "
            "VALUES (:unit,:data,:created_at,:arquivos,:records,:conferencia,:conferido,:pdv_base,:origem,:autor)",
            row,
        )
        return int(cur.lastrowid)


def list_snapshots(unit: str, unit_dir: Path, date_from: str | None = None, date_to: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
    """Lista snapshots sem o payload pesado. Retorna meta + contagens."""
    sql = (
        "SELECT id, unit, data, created_at, arquivos, origem, autor, "
        "       length(records) AS records_size, "
        "       (SELECT json_array_length(s.records) FROM planilhas_snapshot s WHERE s.id = planilhas_snapshot.id) AS records_count "
        "FROM planilhas_snapshot WHERE unit=? "
    )
    params: list[Any] = [unit]
    if date_from:
        sql += "AND data >= ? "
        params.append(date_from)
    if date_to:
        sql += "AND data <= ? "
        params.append(date_to)
    sql += "ORDER BY created_at DESC LIMIT ?"
    params.append(int(limit))
    with _connect(unit_dir) as conn:
        rows = conn.execute(sql, params).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["arquivos"] = json.loads(d.get("arquivos") or "[]")
        except Exception:
            d["arquivos"] = []
        out.append(d)
    return out


def load_snapshot(unit: str, unit_dir: Path, snapshot_id: int) -> dict[str, Any] | None:
    with _connect(unit_dir) as conn:
        r = conn.execute(
            "SELECT * FROM planilhas_snapshot WHERE unit=? AND id=?",
            (unit, int(snapshot_id)),
        ).fetchone()
    if not r:
        return None
    d = dict(r)
    for k in ("arquivos", "records", "conferencia", "conferido", "pdv_base"):
        try:
            d[k] = json.loads(d.get(k) or "null")
        except Exception:
            pass
    return d


def delete_snapshot(unit: str, unit_dir: Path, snapshot_id: int) -> bool:
    with _connect(unit_dir) as conn:
        cur = conn.execute("DELETE FROM planilhas_snapshot WHERE unit=? AND id=?", (unit, int(snapshot_id)))
        return cur.rowcount > 0
