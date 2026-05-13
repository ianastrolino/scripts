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
    id          TEXT PRIMARY KEY,
    unit        TEXT NOT NULL,
    data        TEXT NOT NULL,
    hora        TEXT NOT NULL,
    timestamp   TEXT NOT NULL,
    placa       TEXT NOT NULL,
    cliente     TEXT NOT NULL,
    servico     TEXT NOT NULL,
    valor       REAL NOT NULL,
    fp          TEXT NOT NULL,
    cpf         TEXT NOT NULL DEFAULT "",
    client_uuid TEXT NOT NULL DEFAULT "",
    usuario     TEXT NOT NULL DEFAULT "",
    cv          TEXT NOT NULL DEFAULT ""
);
CREATE INDEX IF NOT EXISTS idx_lancamentos_unit_data ON lancamentos(unit, data);
"""

# Indices que dependem de colunas adicionadas via ALTER. Criados APOS _ensure_column
# em _connect — senao, em bancos antigos, CREATE INDEX falha com "no such column"
# porque o ALTER ainda nao rodou.
_DDL_INDICES_POS_MIGRATE = """
CREATE INDEX IF NOT EXISTS idx_lancamentos_client_uuid ON lancamentos(unit, data, client_uuid);
"""

_MIGRATE_CPF = "ALTER TABLE lancamentos ADD COLUMN cpf TEXT NOT NULL DEFAULT \"\""
_MIGRATE_CLIENT_UUID = "ALTER TABLE lancamentos ADD COLUMN client_uuid TEXT NOT NULL DEFAULT \"\""
_MIGRATE_USUARIO = "ALTER TABLE lancamentos ADD COLUMN usuario TEXT NOT NULL DEFAULT \"\""
_MIGRATE_CV = "ALTER TABLE lancamentos ADD COLUMN cv TEXT NOT NULL DEFAULT \"\""
# perito = nome do vistoriador (coluna PERITO da planilha Sispevi/Megalaudo).
# Coluna em envios_erp pra base do relatorio por vistoriador (Fase 1).
_MIGRATE_ENVIOS_PERITO = "ALTER TABLE envios_erp ADD COLUMN perito TEXT NOT NULL DEFAULT \"\""

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

# Tabela generica pra envios a ERPs (tiny/omie). Antes chamava envios_tiny;
# em 2026-05-02 renomeada pra envios_erp + coluna 'erp' adicionada (DEFAULT 'tiny'
# pra preservar registros legacy). Migration retroativa em _migrate_envios_tabela.
_DDL_ENVIOS = """
CREATE TABLE IF NOT EXISTS envios_erp (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    unit               TEXT NOT NULL,
    erp                TEXT NOT NULL DEFAULT 'tiny',
    chave_deduplicacao TEXT NOT NULL,
    timestamp          TEXT NOT NULL,
    data_lancamento    TEXT NOT NULL DEFAULT "",
    placa              TEXT NOT NULL DEFAULT "",
    cliente            TEXT NOT NULL DEFAULT "",
    servico            TEXT NOT NULL DEFAULT "",
    valor              REAL NOT NULL DEFAULT 0,
    fp                 TEXT NOT NULL DEFAULT "",
    status             TEXT NOT NULL,
    arquivo            TEXT NOT NULL DEFAULT "",
    linha              INTEGER NOT NULL DEFAULT 0,
    resposta_tiny      TEXT NOT NULL DEFAULT "",
    erro               TEXT NOT NULL DEFAULT "",
    perito             TEXT NOT NULL DEFAULT "",
    UNIQUE(unit, chave_deduplicacao)
);
CREATE INDEX IF NOT EXISTS idx_envios_unit_data ON envios_erp(unit, data_lancamento);
CREATE INDEX IF NOT EXISTS idx_envios_unit_ts   ON envios_erp(unit, timestamp);
CREATE INDEX IF NOT EXISTS idx_envios_status    ON envios_erp(unit, status);
CREATE INDEX IF NOT EXISTS idx_envios_unit_erp  ON envios_erp(unit, erp);
"""

# Indice em perito criado DEPOIS do _ensure_column — bancos legacy nao tem
# a coluna no momento do DDL_ENVIOS.
_DDL_INDICE_PERITO = """
CREATE INDEX IF NOT EXISTS idx_envios_unit_perito ON envios_erp(unit, perito);
"""

# Tabela dedicada ao relatorio de vistoriadores (aba "Vistoriadores").
# Independente de envios_erp — registra vistorias da planilha importada,
# mesmo que nunca cheguem ao ERP. Base pra ranking, comparativo e metas.
# Dedup por (unit, data, placa, servico) — reimport atualiza demais campos.
_DDL_VISTORIAS_PLANILHA = """
CREATE TABLE IF NOT EXISTS vistorias_planilha (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    unit         TEXT NOT NULL,
    data         TEXT NOT NULL,
    placa        TEXT NOT NULL,
    cliente      TEXT NOT NULL DEFAULT "",
    servico      TEXT NOT NULL DEFAULT "",
    valor        REAL NOT NULL DEFAULT 0,
    fp           TEXT NOT NULL DEFAULT "",
    perito       TEXT NOT NULL DEFAULT "",
    arquivo      TEXT NOT NULL DEFAULT "",
    importado_em TEXT NOT NULL,
    UNIQUE(unit, data, placa, servico)
);
CREATE INDEX IF NOT EXISTS idx_visto_unit_data    ON vistorias_planilha(unit, data);
CREATE INDEX IF NOT EXISTS idx_visto_unit_perito  ON vistorias_planilha(unit, perito);
CREATE INDEX IF NOT EXISTS idx_visto_unit_servico ON vistorias_planilha(unit, servico);
"""

_DDL_HISTORICO_TINY = """
CREATE TABLE IF NOT EXISTS historico_tiny (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    unit          TEXT NOT NULL,
    id_tiny       TEXT NOT NULL,
    data          TEXT NOT NULL DEFAULT "",
    cliente       TEXT NOT NULL DEFAULT "",
    categoria_id  TEXT NOT NULL DEFAULT "",
    categoria     TEXT NOT NULL DEFAULT "",
    servico_norm  TEXT NOT NULL DEFAULT "",
    valor         REAL NOT NULL DEFAULT 0,
    historico     TEXT NOT NULL DEFAULT "",
    fetched_at    TEXT NOT NULL,
    UNIQUE(unit, id_tiny)
);
CREATE INDEX IF NOT EXISTS idx_hist_unit_data ON historico_tiny(unit, data);
CREATE INDEX IF NOT EXISTS idx_hist_unit_cat  ON historico_tiny(unit, servico_norm);
"""

# Colunas extras (vindas do XLS de Contas a Receber do Tiny). Aplicadas via ALTER
# em _connect() — se a coluna ja existe, sqlite levanta OperationalError, que e ignorado.
_MIGRATE_HIST_EXTRA = [
    'ALTER TABLE historico_tiny ADD COLUMN situacao TEXT NOT NULL DEFAULT ""',
    'ALTER TABLE historico_tiny ADD COLUMN forma_recebimento TEXT NOT NULL DEFAULT ""',
    'ALTER TABLE historico_tiny ADD COLUMN meio_recebimento TEXT NOT NULL DEFAULT ""',
    'ALTER TABLE historico_tiny ADD COLUMN data_liquidacao TEXT NOT NULL DEFAULT ""',
    'ALTER TABLE historico_tiny ADD COLUMN valor_recebido REAL NOT NULL DEFAULT 0',
    'ALTER TABLE historico_tiny ADD COLUMN taxas REAL NOT NULL DEFAULT 0',
    'ALTER TABLE historico_tiny ADD COLUMN numero_documento TEXT NOT NULL DEFAULT ""',
]


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r[1] for r in rows}  # r[1] = column name


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    """Adiciona coluna via ALTER se nao existir. Se existir, no-op.
    Se ALTER falhar, re-raise (nao silencia — o dono do banco precisa ver)."""
    import logging
    if column in _table_columns(conn, table):
        return
    try:
        conn.execute(ddl)
        conn.commit()
    except sqlite3.OperationalError as exc:
        # Segunda chance: pode ser race entre workers. Releu schema.
        if column in _table_columns(conn, table):
            return
        logging.getLogger(__name__).error("[caixa_db] falha ao adicionar coluna %s.%s: %s", table, column, exc)
        raise


def _migrate_envios_tiny_to_erp(conn: sqlite3.Connection) -> None:
    """Migration: renomeia envios_tiny -> envios_erp + adiciona coluna 'erp'.

    Banco antigo (pre-2026-05-02) tem tabela envios_tiny. Banco novo cria direto
    envios_erp via DDL. Esta funcao bridge:
    - Se NAO existe envios_erp E existe envios_tiny: renomeia + adiciona erp
    - Se existem ambas: copia dados que faltam de envios_tiny pra envios_erp
      (improvavel — so se tabelas foram criadas em ordem estranha)
    - Se so existe envios_erp: no-op

    Idempotente. Pode rodar varias vezes sem efeito colateral.
    """
    import logging
    log = logging.getLogger(__name__)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "envios_erp" in tables:
        # Ja migrado. Garante que coluna 'erp' existe (caso sqlite antigo)
        if "erp" not in _table_columns(conn, "envios_erp"):
            try:
                conn.execute("ALTER TABLE envios_erp ADD COLUMN erp TEXT NOT NULL DEFAULT 'tiny'")
                conn.commit()
            except sqlite3.OperationalError:
                pass
        return
    if "envios_tiny" in tables:
        log.info("[caixa_db] migrating envios_tiny -> envios_erp")
        try:
            # SQLite 3.25+ aceita RENAME TABLE. Railway usa sqlite recente.
            conn.execute("ALTER TABLE envios_tiny RENAME TO envios_erp")
            # Coluna erp pode nao existir na tabela antiga
            if "erp" not in _table_columns(conn, "envios_erp"):
                conn.execute("ALTER TABLE envios_erp ADD COLUMN erp TEXT NOT NULL DEFAULT 'tiny'")
            conn.commit()
            log.info("[caixa_db] migration envios_tiny -> envios_erp ok")
        except sqlite3.OperationalError as exc:
            log.error("[caixa_db] migration envios_tiny -> envios_erp falhou: %s", exc)
            # Nao re-raise: o DDL principal cria envios_erp vazia e seguimos.
            # Dados antigos ficam na envios_tiny ate intervencao manual.


def _connect(unit_dir: Path) -> sqlite3.Connection:
    db_path = unit_dir / "caixa_dia.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_DDL)
    _ensure_column(conn, "lancamentos", "cpf",         _MIGRATE_CPF)
    _ensure_column(conn, "lancamentos", "client_uuid", _MIGRATE_CLIENT_UUID)
    _ensure_column(conn, "lancamentos", "usuario",     _MIGRATE_USUARIO)
    # cv = Codigo de Verificacao do cartao (debito/credito) — operador digita
    # do comprovante, vai parar no historico Tiny pra rastreio financeiro
    _ensure_column(conn, "lancamentos", "cv",          _MIGRATE_CV)
    conn.executescript(_DDL_INDICES_POS_MIGRATE)
    conn.executescript(_DDL_DIV)
    conn.executescript(_DDL_SNAPSHOT)
    # Roda migration ANTES do DDL_ENVIOS — renomeia envios_tiny → envios_erp
    # se necessario, pra que o DDL nao crie envios_erp vazia ao lado da legacy
    _migrate_envios_tiny_to_erp(conn)
    conn.executescript(_DDL_ENVIOS)
    # Migration retroativa pro relatorio de peritos (Fase 1 — 2026-05-13).
    # Bancos pre-existentes nao tem a coluna; adiciona via ALTER. Idempotente.
    _ensure_column(conn, "envios_erp", "perito", _MIGRATE_ENVIOS_PERITO)
    # Indice em perito SO POSSO criar depois do _ensure_column — bancos legacy
    # nao tem a coluna no momento do DDL_ENVIOS.
    conn.executescript(_DDL_INDICE_PERITO)
    # Tabela vistorias_planilha (aba Vistoriadores). Idempotente.
    conn.executescript(_DDL_VISTORIAS_PLANILHA)
    conn.executescript(_DDL_HISTORICO_TINY)
    existing_hist = _table_columns(conn, "historico_tiny")
    for sql in _MIGRATE_HIST_EXTRA:
        # extrai nome da coluna do DDL: "ALTER TABLE historico_tiny ADD COLUMN <col> ..."
        parts = sql.split()
        col = parts[parts.index("COLUMN") + 1] if "COLUMN" in parts else ""
        if col and col in existing_hist:
            continue
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass  # race entre workers
    conn.commit()
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
            "(id, unit, data, hora, timestamp, placa, cliente, servico, valor, fp, cpf, cv) "
            "VALUES (:id,:unit,:data,:hora,:timestamp,:placa,:cliente,:servico,:valor,:fp,:cpf,:cv)",
            {**lancamento, "cpf": lancamento.get("cpf", ""), "cv": lancamento.get("cv", "")},
        )


def update_lancamento(unit_dir: Path, lancamento_id: str, fields: dict[str, Any]) -> bool:
    """Atualiza campos de um lançamento. Retorna True se encontrado."""
    allowed = {"placa", "cliente", "servico", "valor", "fp", "cpf", "cv"}
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


# ── Envios Tiny (historico de todos os envios: ok, pulado, falha) ─────────────

def insert_envio_tiny(unit: str, unit_dir: Path, payload: dict[str, Any]) -> bool:
    """Grava um envio na tabela envios_erp. Retorna True se inseriu novo, False se ja existia.

    Nome historico mantido (insert_envio_tiny). Aceita payload['erp'] = 'tiny'
    (default) ou 'omie'. Coluna 'resposta_tiny' guarda resposta crua de
    qualquer ERP — nome legado mas conteudo e generico.
    """
    erp_kind = (payload.get("erp") or "tiny").lower()
    row = {
        "unit":               unit,
        "erp":                erp_kind,
        "chave_deduplicacao": payload.get("chave_deduplicacao", ""),
        "timestamp":          payload.get("timestamp", ""),
        "data_lancamento":    payload.get("data_lancamento", ""),
        "placa":              payload.get("placa", ""),
        "cliente":            payload.get("cliente", ""),
        "servico":            payload.get("servico", ""),
        "valor":              float(payload.get("valor", 0) or 0),
        "fp":                 payload.get("fp", ""),
        "status":             payload.get("status", ""),
        "arquivo":            payload.get("arquivo", ""),
        "linha":              int(payload.get("linha", 0) or 0),
        "resposta_tiny":      json.dumps(payload.get("resposta_tiny") or payload.get("resposta_erp"), ensure_ascii=False) if (payload.get("resposta_tiny") is not None or payload.get("resposta_erp") is not None) else "",
        "erro":               payload.get("erro", ""),
        "perito":             payload.get("perito", ""),
    }
    with _connect(unit_dir) as conn:
        try:
            conn.execute(
                "INSERT INTO envios_erp "
                "(unit, erp, chave_deduplicacao, timestamp, data_lancamento, placa, cliente, servico, valor, fp, status, arquivo, linha, resposta_tiny, erro, perito) "
                "VALUES (:unit,:erp,:chave_deduplicacao,:timestamp,:data_lancamento,:placa,:cliente,:servico,:valor,:fp,:status,:arquivo,:linha,:resposta_tiny,:erro,:perito)",
                row,
            )
            return True
        except sqlite3.IntegrityError:
            # Ja existia — atualiza campos que vieram preenchidos agora.
            # Guard: so sobrescreve se o novo row tem dado; nao apaga dado bom com vazio.
            sets = []
            params = {"unit": row["unit"], "chave": row["chave_deduplicacao"]}
            for col in ("erp", "data_lancamento", "placa", "cliente", "servico", "fp", "arquivo", "resposta_tiny", "erro", "perito"):
                val = row.get(col, "")
                if val:
                    sets.append(f"{col}=:{col}")
                    params[col] = val
            if float(row.get("valor", 0) or 0) > 0:
                sets.append("valor=:valor")
                params["valor"] = row["valor"]
            if row.get("linha"):
                sets.append("linha=:linha")
                params["linha"] = row["linha"]
            if row.get("status"):
                sets.append("status=:status")
                params["status"] = row["status"]
            if row.get("timestamp"):
                sets.append("timestamp=:timestamp")
                params["timestamp"] = row["timestamp"]
            if sets:
                conn.execute(
                    "UPDATE envios_erp SET " + ", ".join(sets) +
                    " WHERE unit=:unit AND chave_deduplicacao=:chave",
                    params,
                )
            return False


def has_envio_tiny(unit: str, unit_dir: Path, chave_deduplicacao: str) -> bool:
    with _connect(unit_dir) as conn:
        r = conn.execute(
            "SELECT 1 FROM envios_erp WHERE unit=? AND chave_deduplicacao=? LIMIT 1",
            (unit, chave_deduplicacao),
        ).fetchone()
    return r is not None


def list_envios_tiny(unit: str, unit_dir: Path, date_from: str | None = None,
                     date_to: str | None = None, status: str | None = None,
                     erp: str | None = None,
                     limit: int = 500) -> list[dict[str, Any]]:
    """Lista envios da tabela envios_erp. Filtro 'erp' opcional (None = qualquer)."""
    sql = "SELECT * FROM envios_erp WHERE unit=? "
    params: list[Any] = [unit]
    if date_from:
        sql += "AND data_lancamento >= ? "
        params.append(date_from)
    if date_to:
        sql += "AND data_lancamento <= ? "
        params.append(date_to)
    if status:
        sql += "AND status = ? "
        params.append(status)
    if erp:
        sql += "AND erp = ? "
        params.append(erp)
    sql += "ORDER BY timestamp DESC LIMIT ?"
    params.append(int(limit))
    with _connect(unit_dir) as conn:
        rows = conn.execute(sql, params).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        if d.get("resposta_tiny"):
            try:
                d["resposta_tiny"] = json.loads(d["resposta_tiny"])
            except Exception:
                pass
        out.append(d)
    return out


# ───────────────────────── vistorias_planilha (aba Vistoriadores) ─────────────
# Tabela alimentada quando o operador importa a planilha (independente de envio
# pro ERP). Base do ranking, comparativo e metas por perito.

def upsert_vistorias_planilha(unit: str, unit_dir: Path,
                              vistorias: list[dict[str, Any]]) -> dict[str, int]:
    """Insere/atualiza vistorias da planilha importada. Dedup por
    (unit, data, placa, servico) — reimport atualiza demais campos.

    Retorna {"inseridas": N, "atualizadas": M}.
    """
    import datetime as _dt
    now = _dt.datetime.now().isoformat(timespec="seconds")
    inseridas = 0
    atualizadas = 0
    with _connect(unit_dir) as conn:
        for v in vistorias:
            data    = (v.get("data") or "").strip()
            placa   = (v.get("placa") or "").strip().upper()
            servico = (v.get("servico") or "").strip().upper()
            if not (data and placa and servico):
                continue  # skip malformados
            row = {
                "unit":         unit,
                "data":         data[:10],
                "placa":        placa,
                "cliente":      (v.get("cliente") or "").strip().upper(),
                "servico":      servico,
                "valor":        float(v.get("valor", 0) or 0),
                "fp":           (v.get("fp") or "").strip().upper(),
                "perito":       (v.get("perito") or "").strip().upper(),
                "arquivo":      v.get("arquivo", ""),
                "importado_em": now,
            }
            try:
                conn.execute(
                    "INSERT INTO vistorias_planilha "
                    "(unit, data, placa, cliente, servico, valor, fp, perito, arquivo, importado_em) "
                    "VALUES (:unit,:data,:placa,:cliente,:servico,:valor,:fp,:perito,:arquivo,:importado_em)",
                    row,
                )
                inseridas += 1
            except sqlite3.IntegrityError:
                # Reimport: atualiza campos preenchidos. Guard: vazio NAO sobrescreve.
                sets = ["importado_em=:importado_em"]
                params = {
                    "unit": row["unit"], "data": row["data"],
                    "placa": row["placa"], "servico": row["servico"],
                    "importado_em": now,
                }
                for col in ("cliente", "fp", "perito", "arquivo"):
                    val = row.get(col, "")
                    if val:
                        sets.append(f"{col}=:{col}")
                        params[col] = val
                if float(row.get("valor", 0) or 0) > 0:
                    sets.append("valor=:valor")
                    params["valor"] = row["valor"]
                conn.execute(
                    "UPDATE vistorias_planilha SET " + ", ".join(sets) +
                    " WHERE unit=:unit AND data=:data AND placa=:placa AND servico=:servico",
                    params,
                )
                atualizadas += 1
        conn.commit()
    return {"inseridas": inseridas, "atualizadas": atualizadas}


def load_vistorias_planilha(unit: str, unit_dir: Path,
                            date_from: str, date_to: str,
                            perito: str | None = None,
                            servico: str | None = None) -> list[dict[str, Any]]:
    """Le vistorias_planilha no periodo. Filtros opcionais por perito e servico.
    Servico aceita substring (LIKE %x%) pra agrupar variações.
    """
    sql = ("SELECT data, placa, cliente, servico, valor, fp, perito, arquivo "
           "FROM vistorias_planilha "
           "WHERE unit=? AND data>=? AND data<=? ")
    params: list[Any] = [unit, date_from, date_to]
    if perito:
        sql += "AND perito = ? "
        params.append(perito.upper())
    if servico:
        sql += "AND servico LIKE ? "
        params.append(f"%{servico.upper()}%")
    sql += "ORDER BY data DESC, placa ASC"
    with _connect(unit_dir) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def load_envios_validos_range(unit: str, unit_dir: Path, date_from: str, date_to: str,
                              erp: str | None = None) -> list[dict[str, Any]]:
    """Lê envios_erp que representam registros efetivamente no ERP (exclui falhas).
    Usado pelo Gerencial/Histórico como fonte financeira consolidada.

    Filtro 'erp' opcional — None retorna tiny+omie consolidados (default pra
    relatorios que mostram total da rede).
    """
    sql = ("SELECT data_lancamento, placa, cliente, servico, valor, fp, status, timestamp, erp "
           "FROM envios_erp "
           "WHERE unit=? AND data_lancamento>=? AND data_lancamento<=? AND status!='falha' ")
    params: list[Any] = [unit, date_from, date_to]
    if erp:
        sql += "AND erp = ? "
        params.append(erp)
    sql += "ORDER BY data_lancamento ASC"
    with _connect(unit_dir) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def upsert_historico_tiny(unit: str, unit_dir: Path, row: dict[str, Any]) -> bool:
    """Insere ou atualiza registro em historico_tiny. Retorna True se novo, False se atualizou.

    Aceita campos extra vindos do XLS (situacao, forma_recebimento, meio_recebimento,
    data_liquidacao, valor_recebido, taxas, numero_documento). Se ausentes, vira "" / 0.
    No UPDATE, campos extras vazios NAO sobrescrevem valores ja salvos — assim o sync
    pela API (que nao tem esses dados) nao apaga o que o XLS trouxe.
    """
    payload = {
        "unit":              unit,
        "id_tiny":           str(row.get("id_tiny", "")),
        "data":              row.get("data", ""),
        "cliente":           row.get("cliente", ""),
        "categoria_id":      str(row.get("categoria_id", "")),
        "categoria":         row.get("categoria", ""),
        "servico_norm":      row.get("servico_norm", ""),
        "valor":             float(row.get("valor", 0) or 0),
        "historico":         row.get("historico", ""),
        "fetched_at":        row.get("fetched_at", ""),
        "situacao":          row.get("situacao", ""),
        "forma_recebimento": row.get("forma_recebimento", ""),
        "meio_recebimento":  row.get("meio_recebimento", ""),
        "data_liquidacao":   row.get("data_liquidacao", ""),
        "valor_recebido":    float(row.get("valor_recebido", 0) or 0),
        "taxas":             float(row.get("taxas", 0) or 0),
        "numero_documento":  row.get("numero_documento", ""),
    }
    with _connect(unit_dir) as conn:
        try:
            conn.execute(
                "INSERT INTO historico_tiny "
                "(unit,id_tiny,data,cliente,categoria_id,categoria,servico_norm,valor,historico,fetched_at,"
                " situacao,forma_recebimento,meio_recebimento,data_liquidacao,valor_recebido,taxas,numero_documento) "
                "VALUES (:unit,:id_tiny,:data,:cliente,:categoria_id,:categoria,:servico_norm,:valor,:historico,:fetched_at,"
                " :situacao,:forma_recebimento,:meio_recebimento,:data_liquidacao,:valor_recebido,:taxas,:numero_documento)",
                payload,
            )
            return True
        except sqlite3.IntegrityError:
            # Preserva campos extra quando row nao os traz: usa COALESCE com valor atual
            # via expressao condicional (sqlite: IIF).
            conn.execute(
                "UPDATE historico_tiny SET "
                "  data=:data, cliente=:cliente, categoria_id=:categoria_id, "
                "  categoria=:categoria, servico_norm=:servico_norm, valor=:valor, historico=:historico, "
                "  fetched_at=:fetched_at, "
                "  situacao=IIF(:situacao='', situacao, :situacao), "
                "  forma_recebimento=IIF(:forma_recebimento='', forma_recebimento, :forma_recebimento), "
                "  meio_recebimento=IIF(:meio_recebimento='', meio_recebimento, :meio_recebimento), "
                "  data_liquidacao=IIF(:data_liquidacao='', data_liquidacao, :data_liquidacao), "
                "  valor_recebido=IIF(:valor_recebido=0, valor_recebido, :valor_recebido), "
                "  taxas=IIF(:taxas=0, taxas, :taxas), "
                "  numero_documento=IIF(:numero_documento='', numero_documento, :numero_documento) "
                "WHERE unit=:unit AND id_tiny=:id_tiny",
                payload,
            )
            return False


def load_historico_tiny_mes(unit: str | None, unit_dir: Path, ano_mes: str) -> list[dict[str, Any]]:
    """Carrega registros de historico_tiny de um mês (ex: '2026-04'). unit=None traz todas."""
    sql = "SELECT * FROM historico_tiny WHERE data LIKE ? "
    params: list[Any] = [f"{ano_mes}-%"]
    if unit:
        sql += "AND unit=? "
        params.append(unit)
    sql += "ORDER BY data ASC"
    with _connect(unit_dir) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def count_historico_tiny(unit: str, unit_dir: Path, ano_mes: str | None = None) -> int:
    sql = "SELECT COUNT(*) AS c FROM historico_tiny WHERE unit=?"
    params: list[Any] = [unit]
    if ano_mes:
        sql += " AND data LIKE ?"
        params.append(f"{ano_mes}-%")
    with _connect(unit_dir) as conn:
        r = conn.execute(sql, params).fetchone()
    return int(r["c"] if r else 0)


def count_envios_tiny(unit: str, unit_dir: Path, erp: str | None = None) -> dict[str, int]:
    """Conta envios por status. Filtro opcional por erp (None = todos)."""
    sql = "SELECT status, COUNT(*) AS c FROM envios_erp WHERE unit=? "
    params: list[Any] = [unit]
    if erp:
        sql += "AND erp = ? "
        params.append(erp)
    sql += "GROUP BY status"
    with _connect(unit_dir) as conn:
        rows = conn.execute(sql, params).fetchall()
    return {r["status"]: r["c"] for r in rows}


def migrate_imported_json_to_envios(unit: str, unit_dir: Path) -> dict[str, int]:
    """Le unit_dir/imported.json e popula envios_erp (com erp='tiny').
    Nao remove o JSON (seguranca).

    Retorna {migrados, duplicados, invalidos}.
    """
    json_path = unit_dir / "imported.json"
    if not json_path.exists():
        return {"migrados": 0, "duplicados": 0, "invalidos": 0}
    try:
        raw = json.loads(json_path.read_text())
    except Exception:
        return {"migrados": 0, "duplicados": 0, "invalidos": 0}
    imported = raw.get("imported", {}) if isinstance(raw, dict) else {}
    migrados = 0
    duplicados = 0
    invalidos = 0
    for chave, meta in imported.items():
        if not isinstance(meta, dict):
            invalidos += 1
            continue
        status = "ja_existia_tiny" if meta.get("motivo", "").startswith("ja existia") else "enviado"
        row = {
            "chave_deduplicacao": chave,
            "timestamp":          meta.get("enviado_em", ""),
            "data_lancamento":    (meta.get("enviado_em", "") or "")[:10],
            "arquivo":            meta.get("arquivo", ""),
            "linha":              meta.get("linha", 0) or 0,
            "status":             status,
            "resposta_tiny":      meta.get("resposta"),
        }
        if insert_envio_tiny(unit, unit_dir, row):
            migrados += 1
        else:
            duplicados += 1
    return {"migrados": migrados, "duplicados": duplicados, "invalidos": invalidos}
