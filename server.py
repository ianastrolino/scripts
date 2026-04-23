#!/usr/bin/env python3
"""
server.py — Flask multi-unit server para Frente de Caixa Astrovistorias.
Deploy no Railway com as variaveis de ambiente: SECRET_KEY, USERS_CONFIG,
UNITS_CONFIG e DATA_DIR (volume persistente).

USERS_CONFIG (JSON):
  {
    "usuario@astrovistorias.com.br": {
      "password_hash": "<saida de criar_usuario.py>",
      "unit": "moema",
      "master": false,
      "gerencial": false,
      "name": "Nome do Usuario"
    }
  }
  master=true  → acessa todas as unidades + gerencial de todas
  gerencial=true → acessa gerencial da sua unidade (master da unidade)

UNITS_CONFIG (JSON):
  {
    "moema": {
      "nome": "Moema",
      "client_id": "...",
      "client_secret": "...",
      "refresh_token": "...",
      "redirect_uri": "https://app.railway.app/u/moema/callback",
      "forma_recebimento_ids": {"FA": 802165201, "dinheiro": 556498207, "debito": 556498211, "credito": 556498209, "pix": 556498217},
      "cliente_ids": {"MARIN IMPORT": 566890464, ...},
      "categoria_id": null,
      "categoria_ids": {"VISTORIA CAUTELAR": 802154986, "LAUDO DE TRANSFERENCIA": 802154835, "LAUDO DE VERIFICACAO": 842630772, "LAUDO CAUTELAR VERIFICACAO": 842630772, "CAUTELAR COM ANALISE": 842630772},
      "vencimento_dias": 0,
      "vencimento_tipo": "ultimo_dia_mes",
      "include_forma_recebimento": true,
      "auto_create_contacts": false,
      "require_payment_mapping": false,
      "default_tipo_pessoa": "J",
      "numero_documento_prefix": "PLANILHA",
      "aliases": {
        "servico": {"LAUDO DE VERIFICACA": "LAUDO DE VERIFICACAO", "LAUDO CAUTELAR VERI": "LAUDO CAUTELAR VERIFICACAO", "CAUTELAR COM ANALIS": "CAUTELAR COM ANALISE", "LAUDO CAUTELAR": "VISTORIA CAUTELAR"},
        "fp": {},
        "cliente": {}
      }
    }
  }
"""
from __future__ import annotations

import collections
import datetime as dt
from zoneinfo import ZoneInfo
import hashlib
import json
import os
import secrets
import sys
import threading
import time
import traceback

import urllib.parse
from dataclasses import asdict
from functools import wraps
from pathlib import Path
from typing import Any

from flask import Flask, Response, redirect, request, send_file, send_from_directory, session, url_for

from caixa_helpers import FP_VALIDOS, calcular_totais, validar_lancamento
from caixa_db import (migrate_from_json as _db_migrate, load_lancamentos as _db_load,
                       _connect as _db_connect, load_lancamentos_range as _db_load_range,
                       insert_divergencia as _db_insert_div, load_divergencias_range as _db_load_div,
                       insert_snapshot as _db_insert_snap, list_snapshots as _db_list_snap,
                       load_snapshot as _db_load_snap, delete_snapshot as _db_delete_snap,
                       insert_envio_tiny as _db_insert_envio, list_envios_tiny as _db_list_envios,
                       count_envios_tiny as _db_count_envios,
                       load_envios_validos_range as _db_load_envios_range,
                       upsert_historico_tiny as _db_upsert_hist,
                       load_historico_tiny_mes as _db_load_hist_mes,
                       count_historico_tiny as _db_count_hist,
                       migrate_imported_json_to_envios as _db_migrate_imported)

# ── Importa logica de negocio do tiny_import.py ────────────────────────────────
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
from tiny_import import (
    DEFAULT_CONFIG,
    NormalizedRecord,
    TinyImporter,
    _is_doc_already_registered,
    apply_alias,
    build_history,
    clean_text,
    compact_document_number,
    is_av_paid,
    last_day_of_month,
    load_state,
    lookup_config_id,
    merge_config,
    money_as_float,
    record_key,
    resolve_categoria_id,
    save_state,
    similarity_score,
)

# ── Flask ──────────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY") or secrets.token_hex(32)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=bool(os.environ.get("RAILWAY_ENVIRONMENT")),
    PERMANENT_SESSION_LIFETIME=43200,   # 12 horas
    MAX_CONTENT_LENGTH=1 * 1024 * 1024, # 1 MB — rejeita payloads gigantes antes de processar
)

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))

# ── Modo manutencao (flag em arquivo no volume persistente) ───────────────────
_MAINTENANCE_FLAG = DATA_DIR / "maintenance.flag"

def _is_maintenance() -> bool:
    return _MAINTENANCE_FLAG.exists()

def _set_maintenance(on: bool, who: str = "") -> None:
    if on:
        _MAINTENANCE_FLAG.parent.mkdir(parents=True, exist_ok=True)
        _MAINTENANCE_FLAG.write_text(
            json.dumps({
                "ligado_em": dt.datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds"),
                "por": who or "desconhecido",
            }, ensure_ascii=False),
            encoding="utf-8",
        )
    else:
        if _MAINTENANCE_FLAG.exists():
            _MAINTENANCE_FLAG.unlink()


@app.before_request
def _maintenance_gate():
    """Bloqueia rotas operacionais das unidades quando manutencao esta ativa.

    Libera: /master/*, /gerencial/*, /login, /logout, /health, /api/me,
    /api/csrf-token, estaticos de /u/<unit>/<file.ext> (html/css/js/png/ico).
    Bloqueia: /u/<unit>/api/* e /u/<unit>/ (redireciona pra pagina de manutencao).
    """
    # Registra presenca do usuario logado (para painel "Usuarios conectados")
    try:
        email = session.get("email")
        if email:
            user = USERS.get(email)
            if user:
                _mark_user_active(email, user)
    except Exception:
        pass
    if not _is_maintenance():
        return None
    path = request.path or ""
    if path.startswith(("/master", "/gerencial", "/login", "/logout", "/health", "/api/me", "/api/csrf-token")):
        return None
    if path.startswith("/u/"):
        parts = path.split("/", 3)  # ["", "u", "<unit>", "<resto>"]
        resto = parts[3] if len(parts) > 3 else ""
        if resto.startswith("api/"):
            return _json({
                "success": False,
                "error": "Sistema em manutencao. Voltamos 23/04 as 09:00.",
                "maintenance": True,
            }, 503)
        if "." in resto:  # arquivo estatico (js/css/html/png)
            return None
        return Response(
            "<!doctype html><meta charset='utf-8'><title>Manutencao</title>"
            "<style>body{font-family:system-ui;background:#0f1117;color:#e5e7eb;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;padding:24px;text-align:center}"
            ".card{max-width:460px;background:#1a1d27;border:1px solid #2a2e3a;border-radius:16px;padding:40px}"
            "h1{color:#fff;font-size:22px;margin:0 0 12px}p{color:#9ca3af;font-size:14px;line-height:1.6;margin:0}</style>"
            "<div class=card><h1>Sistema em manutencao</h1>"
            "<p>Estamos aplicando melhorias hoje (22/04/2026).<br>"
            "Voltamos amanha, 23/04, as 09:00.</p></div>",
            status=503,
            mimetype="text/html; charset=utf-8",
        )
    return None


# ── Rate limiting para login (5 tentativas / 60s por IP) ──────────────────────
_LOGIN_WINDOW  = 60
_LOGIN_MAX     = 5
_login_attempts: dict[str, list[float]] = collections.defaultdict(list)
_login_lock = threading.Lock()

def _login_rate_check(ip: str) -> bool:
    """Retorna True se o IP pode tentar login, False se bloqueado."""
    now = time.monotonic()
    with _login_lock:
        _login_attempts[ip] = [t for t in _login_attempts[ip] if now - t < _LOGIN_WINDOW]
        if len(_login_attempts[ip]) >= _LOGIN_MAX:
            return False
        _login_attempts[ip].append(now)
        return True

# ── Rate limiting para PIN (10 tentativas / 60s por IP+unidade) ───────────────
_PIN_WINDOW  = 60
_PIN_MAX     = 10
_pin_attempts: dict[str, list[float]] = collections.defaultdict(list)
_pin_lock = threading.Lock()

def _pin_rate_check(unit: str, ip: str) -> bool:
    """Retorna True se ainda pode tentar PIN, False se bloqueado."""
    key = f"{unit}:{ip}"
    now = time.monotonic()
    with _pin_lock:
        _pin_attempts[key] = [t for t in _pin_attempts[key] if now - t < _PIN_WINDOW]
        if len(_pin_attempts[key]) >= _PIN_MAX:
            return False
        _pin_attempts[key].append(now)
        return True

UI_DIR   = _HERE / "frente_caixa"


@app.errorhandler(Exception)
def handle_unhandled_exception(exc):
    """Captura qualquer excecao que escape os try-catch das views e retorna JSON
    em vez da pagina HTML padrao do Flask/Gunicorn.
    Isso tambem expoe o traceback para facilitar o diagnostico."""
    from werkzeug.exceptions import HTTPException
    if isinstance(exc, HTTPException):
        return exc  # deixa redirecionamentos e 404 funcionarem normalmente
    tb = traceback.format_exc()
    print(f"[UNHANDLED] {exc}\n{tb}", file=sys.stderr, flush=True)
    return _json({"success": False, "error": "Erro interno do servidor."}, 500)

# ── Carregamento de config ─────────────────────────────────────────────────────
# Arquivo persistente no volume — substitui USERS_CONFIG env como fonte de verdade.
# Seed a partir do env var ocorre na primeira leitura (migracao suave).
_USERS_FILE = DATA_DIR / "users.json"
_USERS_LOCK = threading.Lock()


def _load_users() -> dict[str, Any]:
    """Le usuarios do volume. Se arquivo nao existir, faz seed do USERS_CONFIG env."""
    if _USERS_FILE.exists():
        try:
            return json.loads(_USERS_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    raw = os.environ.get("USERS_CONFIG", "{}")
    try:
        users = json.loads(raw)
    except json.JSONDecodeError:
        users = {}
    try:
        _USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _USERS_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(users, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(_USERS_FILE)
    except OSError:
        pass
    return users


def _save_users(users: dict[str, Any]) -> None:
    """Persiste usuarios no volume (write atomico)."""
    _USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _USERS_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(users, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(_USERS_FILE)


# ── Convites por email ─────────────────────────────────────────────────────────
_INVITES_FILE = DATA_DIR / "convites.json"
_INVITES_LOCK = threading.Lock()
_INVITE_TTL_HOURS = 72


def _load_invites() -> dict[str, Any]:
    if _INVITES_FILE.exists():
        try:
            return json.loads(_INVITES_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_invites(inv: dict[str, Any]) -> None:
    _INVITES_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _INVITES_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(inv, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(_INVITES_FILE)


def _invite_is_valid(invite: dict[str, Any]) -> bool:
    if not invite:
        return False
    if invite.get("usado_em") or invite.get("revogado_em"):
        return False
    try:
        expira = dt.datetime.fromisoformat(invite.get("expira_em", ""))
    except (ValueError, TypeError):
        return False
    now = dt.datetime.now(ZoneInfo("America/Sao_Paulo"))
    if expira.tzinfo is None:
        expira = expira.replace(tzinfo=ZoneInfo("America/Sao_Paulo"))
    return now < expira


def _invite_status(invite: dict[str, Any]) -> str:
    if invite.get("usado_em"):
        return "usado"
    if invite.get("revogado_em"):
        return "revogado"
    if not _invite_is_valid(invite):
        return "expirado"
    return "pendente"


def _load_units() -> dict[str, Any]:
    raw = os.environ.get("UNITS_CONFIG", "{}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


# Carrega uma vez no startup
USERS: dict[str, Any] = _load_users()
UNITS: dict[str, Any] = _load_units()

# ── Auth helpers ───────────────────────────────────────────────────────────────
def _hash_password(password: str) -> str:
    """Gera hash com salt. Use criar_usuario.py para gerar hashes."""
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 200_000)
    return f"{salt}:{dk.hex()}"


def _verify_password(password: str, stored: str) -> bool:
    try:
        salt, dk_hex = stored.split(":", 1)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 200_000)
        return secrets.compare_digest(dk.hex(), dk_hex)
    except Exception:
        return False


def _current_user() -> dict[str, Any] | None:
    email = session.get("email")
    return USERS.get(email) if email else None


# ── Tracking de usuarios conectados ────────────────────────────────────────────
# Dict in-memory + lock. Single-worker basta para o volume atual (< 30 usuarios).
# Se migrar para multi-worker/multi-instance, trocar por SQLite ou Redis.
_ACTIVE_USERS: dict[str, dict[str, Any]] = {}
_ACTIVE_USERS_LOCK = threading.Lock()
_ACTIVE_TTL_SECONDS = 180   # 3 min sem atividade = desconectado

# Log persistente de sessoes (append-only JSONL). Cada linha e uma sessao fechada.
# Nova sessao abre quando o usuario ativa e nao estava em _ACTIVE_USERS (primeiro acesso
# ou retorno apos gap > TTL). Fecha quando expira por inatividade ou logout explicito.
_SESSION_LOG_PATH = DATA_DIR / "session_log.jsonl"
_SESSION_LOG_LOCK = threading.Lock()


def _write_session_log(info: dict[str, Any], reason: str) -> None:
    """Escreve uma linha de sessao encerrada no JSONL."""
    try:
        started_ts = info.get("session_start") or info.get("last_seen")
        ended_ts   = info.get("last_seen")
        if not started_ts or not ended_ts:
            return
        duration = max(0, int(ended_ts - started_ts))
        tz = ZoneInfo("America/Sao_Paulo")
        entry = {
            "email":      info.get("email", ""),
            "nome":       info.get("nome", ""),
            "unit":       info.get("unit", ""),
            "master":     bool(info.get("master")),
            "gerencial":  bool(info.get("gerencial")),
            "started_at": dt.datetime.fromtimestamp(started_ts, tz).isoformat(timespec="seconds"),
            "ended_at":   dt.datetime.fromtimestamp(ended_ts,   tz).isoformat(timespec="seconds"),
            "duration_s": duration,
            "reason":     reason,       # "timeout" | "logout" | "app_restart"
            "last_path":  info.get("last_path", ""),
            "last_ip":    info.get("last_ip", ""),
        }
        _SESSION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _SESSION_LOG_LOCK:
            with _SESSION_LOG_PATH.open("a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as exc:
        app.logger.error("[session_log] falha ao gravar sessao: %s", exc)


def _mark_user_active(email: str, user: dict[str, Any]) -> None:
    if not email:
        return
    now_ts = time.time()
    with _ACTIVE_USERS_LOCK:
        existing = _ACTIVE_USERS.get(email)
        # Nova sessao se: nao estava online OU estava mas com gap > TTL (deveria ter
        # expirado mas _get_active_users ainda nao tinha rodado o cleanup)
        if existing and (now_ts - existing["last_seen"]) <= _ACTIVE_TTL_SECONDS:
            session_start = existing["session_start"]
        else:
            session_start = now_ts
            if existing:
                # Gap longo — fecha a sessao antiga antes de abrir nova
                _write_session_log(existing, "timeout")
        _ACTIVE_USERS[email] = {
            "email":    email,
            "nome":     user.get("name", email),
            "unit":     user.get("unit", ""),
            "master":   bool(user.get("master")),
            "gerencial": bool(user.get("gerencial") or user.get("master")),
            "last_seen":     now_ts,
            "session_start": session_start,
            "last_path": (request.path or "")[:120],
            "last_ip":   request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip(),
        }

def _get_active_users() -> list[dict[str, Any]]:
    cutoff = time.time() - _ACTIVE_TTL_SECONDS
    with _ACTIVE_USERS_LOCK:
        expirados = [(e, info) for e, info in _ACTIVE_USERS.items() if info["last_seen"] < cutoff]
        for e, info in expirados:
            _write_session_log(info, "timeout")
            _ACTIVE_USERS.pop(e, None)
        return sorted(_ACTIVE_USERS.values(), key=lambda x: -x["last_seen"])


def _end_session_on_logout(email: str) -> None:
    """Fecha a sessao do usuario no JSONL ao fazer logout explicito."""
    if not email:
        return
    with _ACTIVE_USERS_LOCK:
        info = _ACTIVE_USERS.pop(email, None)
    if info:
        _write_session_log(info, "logout")


# ── Audit log (append-only JSONL) ─────────────────────────────────────────────
# Registra toda acao administrativa feita por usuarios com perfil matriz ou
# master. Operadores comuns nao entram no audit (volume alto, pouco util).
_AUDIT_LOG_PATH = DATA_DIR / "audit_log.jsonl"
_AUDIT_LOG_LOCK = threading.Lock()


def _write_audit_log(
    user: dict[str, Any],
    action: str,
    target: str = "",
    payload: dict[str, Any] | None = None,
    result: str = "ok",
    approval_id: str = "",
) -> None:
    """Escreve uma entrada no audit log. Chamado apos acoes administrativas."""
    try:
        tz = ZoneInfo("America/Sao_Paulo")
        entry = {
            "ts":          dt.datetime.now(tz).isoformat(timespec="seconds"),
            "user_email":  user.get("email", ""),
            "user_name":   user.get("name", ""),
            "user_role":   "master" if user.get("master") else ("matriz" if user.get("matriz") else "gerencial"),
            "action":      action,
            "target":      target,
            "payload":     payload or {},
            "result":      result,
            "approval_id": approval_id,
            "ip":          (request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
                            if request else ""),
        }
        _AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _AUDIT_LOG_LOCK:
            with _AUDIT_LOG_PATH.open("a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as exc:
        app.logger.error("[audit_log] falha ao gravar: %s", exc)


def _read_audit_log(limit: int = 500) -> list[dict[str, Any]]:
    """Le as ultimas `limit` entradas do audit log (mais recentes primeiro)."""
    if not _AUDIT_LOG_PATH.exists():
        return []
    try:
        with _AUDIT_LOG_LOCK:
            with _AUDIT_LOG_PATH.open("r", encoding="utf-8") as f:
                lines = f.readlines()
        out: list[dict[str, Any]] = []
        for line in reversed(lines):
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
            if len(out) >= limit:
                break
        return out
    except Exception as exc:
        app.logger.error("[audit_log] falha ao ler: %s", exc)
        return []


# ── Pending approvals (acoes destrutivas que matriz dispara e master aprova) ──
_APPROVALS_PATH = DATA_DIR / "pending_approvals.jsonl"
_APPROVALS_LOCK = threading.Lock()


def _approvals_read_all() -> list[dict[str, Any]]:
    if not _APPROVALS_PATH.exists():
        return []
    try:
        with _APPROVALS_LOCK:
            with _APPROVALS_PATH.open("r", encoding="utf-8") as f:
                lines = f.readlines()
        out = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
        return out
    except Exception as exc:
        app.logger.error("[approvals] falha ao ler: %s", exc)
        return []


def _approvals_write_all(entries: list[dict[str, Any]]) -> None:
    _APPROVALS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _APPROVALS_PATH.with_suffix(".tmp")
    with _APPROVALS_LOCK:
        with tmp.open("w", encoding="utf-8") as f:
            for e in entries:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        tmp.replace(_APPROVALS_PATH)


def _create_pending_approval(
    user: dict[str, Any],
    action: str,
    target: str,
    payload: dict[str, Any],
    description: str = "",
) -> str:
    """Cria um pedido de aprovacao pendente. Retorna o approval_id."""
    tz = ZoneInfo("America/Sao_Paulo")
    approval_id = f"ap_{secrets.token_hex(8)}"
    entry = {
        "id":             approval_id,
        "created_at":     dt.datetime.now(tz).isoformat(timespec="seconds"),
        "requested_by":   user.get("email", ""),
        "requested_name": user.get("name", ""),
        "action":         action,
        "target":         target,
        "payload":        payload,
        "description":    description,
        "status":         "pending",     # pending | approved | rejected
        "reviewed_by":    "",
        "reviewed_at":    "",
        "reason":         "",
    }
    with _APPROVALS_LOCK:
        _APPROVALS_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _APPROVALS_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    _write_audit_log(user, f"approval.request:{action}", target, payload, "pending", approval_id)
    return approval_id


def _approvals_pending_count() -> int:
    return sum(1 for e in _approvals_read_all() if e.get("status") == "pending")


# ── CSRF ───────────────────────────────────────────────────────────────────────
def _get_csrf_token() -> str:
    """Gera e persiste token CSRF na sessão do usuário."""
    if "csrf_token" not in session:
        session["csrf_token"] = secrets.token_hex(32)
    return session["csrf_token"]

def csrf_required(f):
    """Valida X-CSRF-Token em requisições mutáveis (POST/PUT/DELETE)."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if request.method in ("POST", "PUT", "DELETE", "PATCH"):
            token = request.headers.get("X-CSRF-Token", "")
            expected = session.get("csrf_token", "")
            if not expected or not secrets.compare_digest(token, expected):
                return _json({"success": False, "error": "Token CSRF inválido."}, 403)
        return f(*args, **kwargs)
    return wrapper


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not _current_user():
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return wrapper


def _perfil_legivel(user: dict) -> str:
    """String amigavel do perfil atual pra mensagens de erro."""
    if not user: return "sem perfil"
    if user.get("master"):    return "master"
    if user.get("matriz"):    return "matriz"
    if user.get("gerencial"): return "gerencial"
    if user.get("unit"):      return f"operador da unidade {user.get('unit')}"
    return "operador"


def master_only_required(f):
    """Exige login + master: true (acesso global a todas as unidades)."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        user = _current_user()
        if not user:
            if "/api/" in request.path:
                return _json({"success": False, "error": "Sessao expirada. Faca login novamente.", "session_expired": True}, 401)
            return redirect(url_for("login_page"))
        if not user.get("master"):
            msg = f"Esta acao e exclusiva do master. Seu perfil atual e {_perfil_legivel(user)}."
            if "/api/" in request.path:
                return _json({"success": False, "error": msg, "reason": "not_master", "perfil": _perfil_legivel(user)}, 403)
            return Response(msg, status=403)
        return f(*args, **kwargs)
    return wrapper


def gerencial_required(f):
    """Exige login + acesso à unidade + flag gerencial (ou master/matriz global)."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        user = _current_user()
        if not user:
            if "/api/" in request.path:
                return _json({"success": False, "error": "Sessao expirada. Faca login novamente.", "session_expired": True}, 401)
            return redirect(url_for("login_page"))
        unit = kwargs.get("unit")
        is_global = user.get("master") or user.get("matriz")
        if not is_global and user.get("unit") != unit:
            msg = f"Voce nao tem acesso a unidade {unit}. Seu perfil atual e {_perfil_legivel(user)}."
            if "/api/" in request.path:
                return _json({"success": False, "error": msg, "reason": "wrong_unit", "perfil": _perfil_legivel(user)}, 403)
            return Response(msg, status=403)
        if not is_global and not user.get("gerencial"):
            msg = f"Esta tela e restrita ao gerente da unidade. Seu perfil atual e {_perfil_legivel(user)}."
            if "/api/" in request.path:
                return _json({"success": False, "error": msg, "reason": "not_gerencial", "perfil": _perfil_legivel(user)}, 403)
            return Response(msg, status=403)
        return f(*args, **kwargs)
    return wrapper


def unit_access_required(f):
    """Verifica login + acesso a unidade (master e matriz veem todas).
    Rotas /api/ recebem JSON 401 em vez de redirect HTML quando a sessao expira.
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        user = _current_user()
        if not user:
            if "/api/" in request.path:
                return _json({"success": False, "error": "Sessao expirada. Recarregue a pagina e faca login novamente.", "session_expired": True}, 401)
            return redirect(url_for("login_page"))
        unit = kwargs.get("unit")
        is_global = user.get("master") or user.get("matriz")
        if not is_global and user.get("unit") != unit:
            msg = f"Voce nao tem acesso a unidade {unit}. Seu perfil atual e {_perfil_legivel(user)}."
            if "/api/" in request.path:
                return _json({"success": False, "error": msg, "reason": "wrong_unit", "perfil": _perfil_legivel(user)}, 403)
            return Response(msg, status=403)
        return f(*args, **kwargs)
    return wrapper


def master_view_required(f):
    """Read-only: libera master E matriz (ambos veem todas as unidades)."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        user = _current_user()
        if not user:
            if "/api/" in request.path:
                return _json({"success": False, "error": "Sessao expirada. Faca login novamente.", "session_expired": True}, 401)
            return redirect(url_for("login_page"))
        if not (user.get("master") or user.get("matriz")):
            msg = f"Esta tela e restrita a master e matriz. Seu perfil atual e {_perfil_legivel(user)}."
            if "/api/" in request.path:
                return _json({"success": False, "error": msg, "reason": "not_rede", "perfil": _perfil_legivel(user)}, 403)
            return Response(msg, status=403)
        return f(*args, **kwargs)
    return wrapper


def matriz_or_master(f):
    """Write permitido para master E matriz. Use em acoes administrativas
    que ambos podem disparar (criar/editar usuario, editar lancamento, etc).
    Acoes destrutivas (excluir, reenviar ao Tiny) devem ficar em @master_only_required
    e, quando disparadas pela matriz via endpoint proprio, criar pending approval."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        user = _current_user()
        if not user:
            if "/api/" in request.path:
                return _json({"success": False, "error": "Sessao expirada. Faca login novamente.", "session_expired": True}, 401)
            return redirect(url_for("login_page"))
        if not (user.get("master") or user.get("matriz")):
            msg = f"Esta acao e restrita a master e matriz. Seu perfil atual e {_perfil_legivel(user)}."
            if "/api/" in request.path:
                return _json({"success": False, "error": msg, "reason": "not_rede", "perfil": _perfil_legivel(user)}, 403)
            return Response(msg, status=403)
        return f(*args, **kwargs)
    return wrapper


def master_required(f):
    """Restringe rota a usuários com flag master=True. Retorna JSON 403 para os demais."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        user = _current_user()
        if not user:
            return _json({"error": "Nao autenticado."}, 401)
        if not user.get("master"):
            return _json({"error": "Acesso negado."}, 403)
        return f(*args, **kwargs)
    return wrapper


# ── Helpers de unidade ────────────────────────────────────────────────────────
def _unit_state_dir(unit: str) -> Path:
    d = DATA_DIR / unit
    d.mkdir(parents=True, exist_ok=True)
    return d


def _load_extra_cliente_ids(unit: str) -> dict[str, int]:
    """Carrega mapeamentos de clientes salvos via modal (persistidos em JSON)."""
    p = _unit_state_dir(unit) / "cliente_ids.json"
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            return {}
    return {}


def _save_extra_cliente_ids(unit: str, ids: dict[str, int]) -> None:
    """Escrita atomica: evita corrupcao se o processo for morto durante o flush."""
    p = _unit_state_dir(unit) / "cliente_ids.json"
    tmp = p.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(ids, ensure_ascii=False, indent=2))
        tmp.replace(p)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


_GLOBAL_ALIASES: dict[str, dict[str, str]] = {
    # Aliases aplicados em TODAS as unidades. Alias por unidade (UNITS_CONFIG) vence no merge.
    # Sao nomes que representam o mesmo servico — unifica o ranking e o envio ao Tiny.
    "servico": {
        "LAUDO CAUTELAR": "VISTORIA CAUTELAR",
    },
    "fp": {},
    "cliente": {},
}


# Mapa de categorias Tiny por unidade — cada Tiny tem IDs proprios.
# Substitui o categoria_ids do UNITS_CONFIG; serve tambem como lista de servicos
# do dropdown do PDV (api/info deriva isso das chaves de categoria_ids).
# Pra adicionar uma unidade nova, chamar /u/<unit>/api/diagnostic-categorias
# no Tiny dela e preencher aqui.
_CATEGORIAS_POR_UNIDADE: dict[str, dict[str, int]] = {
    "barueri": {
        "VISTORIA CAUTELAR":                       897968382,
        "LAUDO DE TRANSFERENCIA":                  897968383,
        "LAUDO DE VERIFICACAO":                    897968384,
        "LAUDO CAUTELAR VERIFICACAO":              897968384,
        "CAUTELAR COM ANALISE":                    897968384,
        "CAUTELAR COM ANALISE DE PINTURA":         897968384,
        "REVISTORIA":                              897968385,
        "BAIXA PERMANENTE":                        897968385,
        "CONSULTA GRAVAME":                        897968385,
        "EMISSAO CRLV":                            897968394,
        "PESQUISA AVULSA":                         897968391,
        "VISTORIA ESTRUTURAL SEM EMISSAO DE LAUDO": 897968392,
        "TAXA DE VISITA":                          897968393,
    },
    "mooca": {
        "VISTORIA CAUTELAR":                       747861717,
        "LAUDO DE TRANSFERENCIA":                  747861718,
        "LAUDO DE VERIFICACAO":                    747861719,
        "LAUDO CAUTELAR VERIFICACAO":              747861719,
        "CAUTELAR COM ANALISE":                    747861719,
        "CAUTELAR COM ANALISE DE PINTURA":         747861719,
        "REVISTORIA":                              757397501,
        "BAIXA PERMANENTE":                        774162345,
        "CONSULTA GRAVAME":                        786613890,
        "EMISSAO CRLV":                            747861715,
        "PESQUISA AVULSA":                         747861721,
        "VISTORIA ESTRUTURAL SEM EMISSAO DE LAUDO": 747861714,
    },
}


def _categorias_path(unit: str) -> Path:
    return _unit_state_dir(unit) / "categorias.json"


def _load_unit_categorias(unit: str) -> dict[str, int]:
    """Carrega mapa servico→categoria_id da unidade. Fallback pro hardcoded se nao existe."""
    p = _categorias_path(unit)
    if p.exists():
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            # Normaliza: nome upper, id int
            return {str(k).upper().strip(): int(v) for k, v in (raw or {}).items() if str(k).strip() and v}
        except Exception:
            pass
    return dict(_CATEGORIAS_POR_UNIDADE.get(unit, {}))


def _save_unit_categorias(unit: str, mapa: dict[str, int]) -> None:
    """Escrita atomica do arquivo de categorias da unidade."""
    p = _categorias_path(unit)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    normalized = {str(k).upper().strip(): int(v) for k, v in mapa.items() if str(k).strip() and v}
    tmp.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _build_unit_config(unit: str) -> dict[str, Any]:
    """Monta config completo para a unidade a partir de UNITS_CONFIG."""
    ud = UNITS.get(unit, {})
    tiny: dict[str, Any] = {}

    for field in (
        "client_id", "client_secret", "refresh_token", "redirect_uri",
        "forma_recebimento_ids", "cliente_ids", "categoria_id", "categoria_ids",
        "vencimento_dias", "vencimento_tipo", "numero_documento_prefix",
        "require_payment_mapping",
        "include_forma_recebimento", "default_tipo_pessoa",
    ):
        if (v := ud.get(field)) is not None:
            tiny[field] = v

    # auto_create_contacts e forcado true pra toda a rede — ignora qualquer
    # 'false' legado no UNITS_CONFIG. Particulares (AV) nao tem botao 'Mapear
    # cliente' na UI, entao o unico jeito deles passarem eh serem criados
    # automaticamente.
    tiny["auto_create_contacts"] = True

    # Merge dos aliases: globais (base) + da unidade (sobrescrevem em cima)
    unit_aliases = ud.get("aliases", {}) or {}
    merged_aliases: dict[str, dict[str, str]] = {}
    for field in ("servico", "fp", "cliente"):
        merged_aliases[field] = dict(_GLOBAL_ALIASES.get(field, {}))
        merged_aliases[field].update(unit_aliases.get(field, {}) or {})
    tiny["aliases"] = merged_aliases

    # Sobrescreve categoria_ids com o mapa por unidade (IDs do Tiny de cada unidade).
    # Le de /data/<unit>/categorias.json (editavel via UI), com fallback pro hardcoded
    # _CATEGORIAS_POR_UNIDADE quando o arquivo ainda nao foi criado.
    unit_cats = _load_unit_categorias(unit)
    if unit_cats:
        tiny["categoria_ids"] = unit_cats

    # Sobrescreve com IDs salvos via modal de mapeamento
    extra = _load_extra_cliente_ids(unit)
    if extra:
        merged_ids: dict[str, Any] = dict(tiny.get("cliente_ids", {}))
        merged_ids.update(extra)
        tiny["cliente_ids"] = merged_ids

    return merge_config(DEFAULT_CONFIG, {
        "state_dir": str(_unit_state_dir(unit)),
        "tiny": tiny,
    })


def _seed_tokens(unit: str, config: dict[str, Any]) -> None:
    """Bootstrap-only: cria arquivo de tokens a partir do UNITS_CONFIG APENAS se nao existir
    ou estiver vazio/corrompido. Apos a primeira autorizacao OAuth, o arquivo e a
    UNICA fonte de verdade — o refresh_token do Railway nunca sobrescreve um arquivo valido."""
    p = _unit_state_dir(unit) / "tiny_tokens.json"
    if p.exists():
        try:
            stored = json.loads(p.read_text())
            if stored.get("refresh_token"):
                return  # arquivo ja tem token — nao mexe, e o rei
        except Exception:
            pass  # arquivo corrompido — pode sobrescrever com seed
    rt = config["tiny"].get("refresh_token", "")
    if not rt:
        return
    p.write_text(json.dumps({
        "access_token": "",
        "refresh_token": rt,
        "expires_at": 0,
    }))


# ── Resposta JSON helper ───────────────────────────────────────────────────────
def _json(data: Any, status: int = 200) -> Response:
    return app.response_class(
        response=json.dumps(data, ensure_ascii=False),
        status=status,
        mimetype="application/json",
    )


# ══════════════════════════════════════════════════════════════════════════════
# Rota: health check
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/me")
@login_required
def api_me():
    """Retorna dados do usuário logado — usado pela home page."""
    user = _current_user()
    is_master = bool(user and user.get("master"))
    payload = {
        "usuario": session.get("name", ""),
        "email":   session.get("email", ""),
        "master":  is_master,
        "matriz":  bool(user and user.get("matriz")),
        "gerencial": bool(user and (user.get("gerencial") or user.get("master") or user.get("matriz"))),
        "unit":    user.get("unit", "") if user else "",
    }
    # Master ve o badge de aprovacoes pendentes no sidebar
    if is_master:
        payload["pending_approvals"] = _approvals_pending_count()
    return _json(payload)


@app.route("/api/csrf-token")
@login_required
def api_csrf_token():
    """Retorna o token CSRF da sessão — chamado uma vez pelo frontend."""
    return _json({"token": _get_csrf_token()})


@app.route("/health")
def health():
    """Usado pelo Railway para verificar se o processo está vivo."""
    return _json({
        "status": "ok",
        "units":  len(UNITS),
        "ts":     dt.datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds"),
    })


# ══════════════════════════════════════════════════════════════════════════════
# Rotas: autenticacao
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "POST":
        ip = request.remote_addr or "unknown"
        if not _login_rate_check(ip):
            return redirect(url_for("login_page") + "?erro=bloqueado")

        email = (request.form.get("email") or "").strip().lower()
        pw    = request.form.get("password") or ""

        if not email.endswith("@astrovistorias.com.br"):
            return redirect(url_for("login_page") + "?erro=dominio")

        user = USERS.get(email)
        if user and _verify_password(pw, user["password_hash"]):
            session.permanent = True
            session["email"] = email
            session["name"]  = user.get("name", email)
            return redirect(url_for("index"))

        return redirect(url_for("login_page") + "?erro=credenciais")

    return send_from_directory(UI_DIR, "login.html")


@app.route("/logout")
def logout():
    email = (session.get("email") or "").lower()
    _end_session_on_logout(email)
    session.clear()
    return redirect(url_for("login_page"))


@app.route("/<path:filename>")
def root_static(filename: str):
    """Serve arquivos estaticos da UI (logo, css, js) fora das rotas de unidade."""
    return send_from_directory(UI_DIR, filename)


@app.route("/")
@login_required
def index():
    user = _current_user()
    if user.get("master") or user.get("matriz"):
        return redirect("/home")
    unit = user.get("unit")
    if unit:
        return redirect(f"/u/{unit}/home")
    return redirect(url_for("login_page"))


@app.route("/home")
@login_required
def home_master():
    return send_from_directory(UI_DIR, "home.html")


@app.route("/u/<unit>/home")
@unit_access_required
def home_unit(unit: str):
    return send_from_directory(UI_DIR, "home.html")


# ══════════════════════════════════════════════════════════════════════════════
# Rotas: dashboard master
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/master")
@login_required
def master_page():
    user = _current_user()
    if not (user.get("master") or user.get("matriz")):
        unit = user.get("unit")
        return redirect(f"/u/{unit}/") if unit else redirect(url_for("login_page"))
    return send_from_directory(UI_DIR, "master.html")


@app.route("/master/api/units")
@master_view_required
def master_api_units():
    units_info = [
        {"id": uid, "nome": ud.get("nome", uid)}
        for uid, ud in UNITS.items()
    ]
    return _json({"units": units_info})


# ══════════════════════════════════════════════════════════════════════════════
# Categorias Tiny por unidade (CRUD via UI)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/master/api/categorias/<unit>", methods=["GET"])
@master_view_required
def master_api_categorias_list(unit: str):
    if unit not in UNITS:
        return _json({"success": False, "error": "unit invalida"}, 400)
    cats = _load_unit_categorias(unit)
    out = sorted(
        [{"nome": k, "tiny_id": v} for k, v in cats.items()],
        key=lambda c: c["nome"],
    )
    return _json({"success": True, "unit": unit, "categorias": out})


@app.route("/master/api/categorias/<unit>", methods=["POST"])
@master_only_required
@csrf_required
def master_api_categorias_save(unit: str):
    """Cria ou atualiza uma categoria. Body: {nome, tiny_id}."""
    if unit not in UNITS:
        return _json({"success": False, "error": "unit invalida"}, 400)
    data = request.get_json(force=True, silent=True) or {}
    nome = str(data.get("nome", "")).strip().upper()
    try:
        tiny_id = int(data.get("tiny_id", 0))
    except Exception:
        return _json({"success": False, "error": "tiny_id invalido (deve ser numero)"}, 400)
    if not nome or tiny_id <= 0:
        return _json({"success": False, "error": "nome e tiny_id sao obrigatorios"}, 400)
    cats = _load_unit_categorias(unit)
    cats[nome] = tiny_id
    _save_unit_categorias(unit, cats)
    me = _current_user() or {}
    _write_audit_log(me, "categoria.save", f"{unit}:{nome}", {"unit": unit, "nome": nome, "tiny_id": tiny_id})
    return _json({"success": True, "categorias": {"nome": nome, "tiny_id": tiny_id}})


@app.route("/master/api/categorias/<unit>/<path:nome>", methods=["DELETE"])
@master_only_required
@csrf_required
def master_api_categorias_delete(unit: str, nome: str):
    if unit not in UNITS:
        return _json({"success": False, "error": "unit invalida"}, 400)
    cats = _load_unit_categorias(unit)
    key = nome.strip().upper()
    if key not in cats:
        return _json({"success": False, "error": "categoria nao encontrada"}, 404)
    removed_id = cats.pop(key)
    _save_unit_categorias(unit, cats)
    me = _current_user() or {}
    _write_audit_log(me, "categoria.delete", f"{unit}:{key}", {"unit": unit, "nome": key, "tiny_id": removed_id})
    return _json({"success": True})


@app.route("/master/api/categorias/<unit>/importar-tiny", methods=["GET"])
@master_view_required
def master_api_categorias_importar_tiny(unit: str):
    """Lista as categorias cadastradas no Tiny da unidade, pro usuario ticar quais importar."""
    if unit not in UNITS:
        return _json({"success": False, "error": "unit invalida"}, 400)
    try:
        config = _build_unit_config(unit)
        state_dir = _unit_state_dir(unit)
        importer = TinyImporter(config, state_dir)
        res = importer.client.request("GET", "categorias-receita-despesa", params={"limit": 200})
        itens = res.get("itens", [])
        existentes = _load_unit_categorias(unit)
        ja_mapeados = {v for v in existentes.values()}
        saida = []
        for it in itens:
            try:
                tid = int(it.get("id", 0))
            except Exception:
                continue
            if tid <= 0:
                continue
            nome = (it.get("descricao") or it.get("nome") or "").strip().upper()
            if not nome:
                continue
            saida.append({
                "tiny_id": tid,
                "nome_tiny": nome,
                "ja_mapeado": tid in ja_mapeados,
                "tipo": it.get("tipo", ""),
            })
        saida.sort(key=lambda c: c["nome_tiny"])
        return _json({"success": True, "categorias_tiny": saida})
    except Exception as exc:
        app.logger.exception("[categorias.importar-tiny] %s", unit)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/master/api/categorias/<unit>/importar-tiny", methods=["POST"])
@master_only_required
@csrf_required
def master_api_categorias_importar_tiny_save(unit: str):
    """Recebe lista [{nome, tiny_id}, ...] e grava em lote (merge no existente)."""
    if unit not in UNITS:
        return _json({"success": False, "error": "unit invalida"}, 400)
    data = request.get_json(force=True, silent=True) or {}
    itens = data.get("itens") or []
    if not isinstance(itens, list):
        return _json({"success": False, "error": "itens deve ser lista"}, 400)
    cats = _load_unit_categorias(unit)
    count = 0
    for it in itens:
        nome = str(it.get("nome", "")).strip().upper()
        try:
            tid = int(it.get("tiny_id", 0))
        except Exception:
            continue
        if not nome or tid <= 0:
            continue
        cats[nome] = tid
        count += 1
    _save_unit_categorias(unit, cats)
    me = _current_user() or {}
    _write_audit_log(me, "categoria.importar_tiny", unit, {"unit": unit, "total": count})
    return _json({"success": True, "importados": count})


@app.route("/master/categorias")
@master_view_required
def master_categorias_page():
    return _nocache(send_from_directory(UI_DIR, "categorias.html"))


@app.route("/master/api/units/status")
@master_view_required
def master_api_units_status():
    """Resumo operacional do caixa do dia para todas as unidades.

    Retorna por unidade: total de lançamentos, valor total e hora do último
    lançamento. Usado pelo painel master para monitorar 300 unidades de uma vez.
    """
    today = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).date().isoformat()
    status = []
    for uid, ud in UNITS.items():
        try:
            lancamentos = _db_load(uid, _unit_state_dir(uid), today)
        except Exception:
            lancamentos = []
        totais = calcular_totais(lancamentos)
        ultimo_lc = max(lancamentos, key=lambda x: x.get("timestamp", ""), default=None) if lancamentos else None
        ultima = ultimo_lc.get("timestamp", "") if ultimo_lc else None
        ultimo_resumo = None
        if ultimo_lc:
            ultimo_resumo = {
                "placa":   ultimo_lc.get("placa", "") or "",
                "servico": ultimo_lc.get("servico", "") or "",
                "valor":   float(ultimo_lc.get("valor", 0) or 0),
                "fp":      ultimo_lc.get("fp", "") or "",
            }
        status.append({
            "id":   uid,
            "nome": ud.get("nome", uid),
            "hoje": {
                "lancamentos":    len(lancamentos),
                "total":          totais["total"],
                "ultima_atividade": ultima,
                "ultimo": ultimo_resumo,
            },
        })
    return _json({"status": status, "data": today})


@app.route("/master/api/tiny-health")
@master_view_required
def master_api_tiny_health():
    """Status do token Tiny para cada unidade — alimenta o painel master.

    Retorna por unidade:
      - has_token: se existe arquivo de token
      - access_expires_in: segundos ate o access_token expirar (negativo se expirado)
      - refresh_token_tail: ultimos 6 chars do refresh_token (identificacao visual)
      - file_mtime: quando o arquivo foi atualizado pela ultima vez
      - status: "ok" | "renovar" | "ausente" | "erro"
    """
    now = time.time()
    items = []
    for uid in UNITS.keys():
        p = _unit_state_dir(uid) / "tiny_tokens.json"
        entry = {"id": uid, "nome": UNITS[uid].get("nome", uid)}
        if not p.exists():
            entry["status"] = "ausente"
            entry["has_token"] = False
            items.append(entry)
            continue
        try:
            stored = json.loads(p.read_text())
            rt = stored.get("refresh_token", "")
            expires_at = float(stored.get("expires_at", 0) or 0)
            expires_in = int(expires_at - now) if expires_at else None
            entry["has_token"] = bool(rt)
            entry["refresh_token_tail"] = rt[-6:] if rt else ""
            entry["access_expires_in"] = expires_in
            entry["file_mtime"] = dt.datetime.fromtimestamp(p.stat().st_mtime, ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds")
            if not rt:
                entry["status"] = "ausente"
            elif expires_in is not None and expires_in < 0:
                entry["status"] = "renovar"
            else:
                entry["status"] = "ok"
        except Exception as exc:
            entry["status"] = "erro"
            entry["error"] = str(exc)[:200]
        items.append(entry)
    return _json({"units": items, "checked_at": dt.datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds")})


@app.route("/master/api/debug/storage")
@master_required
def master_api_debug_storage():
    """Diagnostico do volume persistente: tamanho total, por unidade, por arquivo.

    Usado para confirmar que DATA_DIR aponta para o volume montado no Railway
    e que os dados estao sendo gravados no disco persistente.
    """
    import shutil as _shutil

    def _dir_stats(path: Path) -> dict:
        total = 0
        files = 0
        biggest: list[tuple[int, str]] = []
        if path.exists():
            for p in path.rglob("*"):
                try:
                    if p.is_file():
                        size = p.stat().st_size
                        total += size
                        files += 1
                        rel = str(p.relative_to(path))
                        biggest.append((size, rel))
                except (OSError, ValueError):
                    continue
        biggest.sort(reverse=True)
        return {
            "bytes": total,
            "human": _human_bytes(total),
            "files": files,
            "top5": [{"size": _human_bytes(s), "path": r} for s, r in biggest[:5]],
        }

    try:
        du = _shutil.disk_usage(str(DATA_DIR if DATA_DIR.exists() else DATA_DIR.parent))
        disk = {
            "total": _human_bytes(du.total),
            "used":  _human_bytes(du.used),
            "free":  _human_bytes(du.free),
            "used_pct": round(du.used / du.total * 100, 1) if du.total else 0,
        }
    except Exception as exc:
        disk = {"error": str(exc)[:200]}

    root = _dir_stats(DATA_DIR)
    per_unit = {}
    if DATA_DIR.exists():
        for child in sorted(DATA_DIR.iterdir()):
            if child.is_dir():
                per_unit[child.name] = _dir_stats(child)

    snapshots_count = 0
    try:
        for uid in UNITS.keys():
            udir = DATA_DIR / uid
            if udir.exists():
                snapshots_count += len(_db_list_snap(uid, udir, limit=10_000))
    except Exception:
        snapshots_count = -1

    return _json({
        "data_dir": str(DATA_DIR),
        "data_dir_exists": DATA_DIR.exists(),
        "disk": disk,
        "total": root,
        "per_unit": per_unit,
        "snapshots_total": snapshots_count,
        "checked_at": dt.datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds"),
    })


@app.route("/gerencial/api/envios-tiny", methods=["GET"])
@master_view_required
def master_api_envios_tiny_list():
    """Lista envios para o Tiny no intervalo. Modo espelho — pode faltar historico antigo
    que ainda esta so no imported.json. Use POST /gerencial/api/envios-tiny/migrate uma vez
    para importar o JSON para a tabela.

    Params: ?unit=<slug>&from=YYYY-MM-DD&to=YYYY-MM-DD&status=<enviado|ja_existia_tiny|ja_importado_local|falha>&limit=500
    """
    unit       = (request.args.get("unit") or "").strip()
    date_from  = (request.args.get("from") or "").strip() or None
    date_to    = (request.args.get("to") or "").strip() or None
    status     = (request.args.get("status") or "").strip() or None
    try:
        limit = max(1, min(int(request.args.get("limit", 500)), 5000))
    except Exception:
        limit = 500

    units_to_query = [unit] if unit and unit in UNITS else list(UNITS.keys())
    out = []
    totals: dict[str, dict[str, int]] = {}
    for uid in units_to_query:
        state_dir = _unit_state_dir(uid)
        try:
            envios = _db_list_envios(uid, state_dir, date_from=date_from, date_to=date_to,
                                      status=status, limit=limit)
            counts = _db_count_envios(uid, state_dir)
        except Exception as exc:
            app.logger.warning("[envios-tiny] falha unit=%s: %s", uid, exc)
            envios = []
            counts = {}
        for e in envios:
            e["unit"] = uid
            e["unit_nome"] = UNITS[uid].get("nome", uid)
        out.extend(envios)
        totals[uid] = counts
    out.sort(key=lambda e: e.get("timestamp", ""), reverse=True)
    return _json({"success": True, "envios": out[:limit], "totais": totals})


@app.route("/gerencial/api/envios-tiny/migrate", methods=["POST"])
@master_required
@csrf_required
def master_api_envios_tiny_migrate():
    """One-shot: le imported.json de cada unidade e popula envios_tiny.
    Idempotente — rodar de novo so insere o que faltava.
    """
    resultado = {}
    for uid in UNITS:
        state_dir = _unit_state_dir(uid)
        try:
            resultado[uid] = _db_migrate_imported(uid, state_dir)
        except Exception as exc:
            app.logger.exception("[envios-tiny:migrate] falha unit=%s", uid)
            resultado[uid] = {"erro": str(exc)}
    app.logger.info("[envios-tiny:migrate] %s", resultado)
    return _json({"success": True, "resultado": resultado})


_BI_CATEGORIAS_BLACKLIST = {"APORTE", "RENDIMENTO", "JUROS", "MULTA", "DEPOSITO", "EMPRESTIMO", "EMPRÉSTIMO"}


def _e_categoria_de_servico(nome: str) -> bool:
    """Filtra categorias que NAO sao servicos (aportes, rendimentos, etc)."""
    up = (nome or "").strip().upper()
    if not up:
        return False
    return not any(termo in up for termo in _BI_CATEGORIAS_BLACKLIST)


@app.route("/gerencial/api/bi/sync-historico", methods=["POST"])
@master_required
@csrf_required
def master_api_bi_sync_historico():
    """Puxa contas a receber do Tiny de uma unidade num mes e salva em historico_tiny.

    Body: { unit: "barueri", mes: "2026-04" }
    """
    try:
        data = request.get_json(force=True, silent=True) or {}
        unit = (data.get("unit") or "").strip()
        mes  = (data.get("mes")  or "").strip()  # "AAAA-MM"
        dia  = (data.get("dia")  or "").strip()  # "AAAA-MM-DD" — se passado, sincroniza so esse dia (rapido)
        if unit not in UNITS:
            return _json({"success": False, "error": "unit invalida"}, 400)
        if dia:
            try:
                data_ini = dt.date.fromisoformat(dia)
                data_fim = data_ini
            except Exception:
                return _json({"success": False, "error": "dia invalido (use AAAA-MM-DD)"}, 400)
        else:
            try:
                ano, mm = mes.split("-")
                ano, mm = int(ano), int(mm)
                data_ini = dt.date(ano, mm, 1)
                data_fim = (data_ini.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1)
            except Exception:
                return _json({"success": False, "error": "mes invalido (use AAAA-MM)"}, 400)

        config    = _build_unit_config(unit)
        state_dir = _unit_state_dir(unit)
        importer  = TinyImporter(config, state_dir)
        client    = importer.client

        # Converte AAAA-MM-DD -> DD/MM/AAAA (formato esperado pelo Tiny)
        def _br(d: dt.date) -> str:
            return d.strftime("%d/%m/%Y")

        # Itera dia a dia — Tiny rejeita ranges longos com 400 quando o
        # volume ultrapassa o limite interno ("seja mais especifico").
        todos: list[dict] = []
        d = data_ini
        while d <= data_fim:
            offset = 0
            limit  = 100
            while True:
                params = {
                    "limit": limit, "offset": offset,
                    "dataInicial": _br(d), "dataFinal": _br(d),
                }
                try:
                    resp = client.request("GET", "contas-receber", params=params)
                except Exception as exc:
                    app.logger.warning("[bi.sync] falha dia=%s: %s", d, exc)
                    break
                page = resp.get("itens", [])
                todos.extend(page)
                if len(page) < limit:
                    break
                offset += limit
            d += dt.timedelta(days=1)

        # GET detalhe em paralelo pra pegar categoria (lista nao retorna)
        from concurrent.futures import ThreadPoolExecutor
        def _fetch_detalhe(item: dict) -> dict | None:
            try:
                return client.request("GET", f"contas-receber/{item['id']}")
            except Exception as exc:
                app.logger.warning("[bi.sync] falha detalhe id=%s: %s", item.get("id"), exc)
                return None

        detalhes: list[dict] = []
        with ThreadPoolExecutor(max_workers=5) as pool:
            for d in pool.map(_fetch_detalhe, todos):
                if d:
                    detalhes.append(d)

        ts_now = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds")
        novos = 0
        atualizados = 0
        for d in detalhes:
            cat = d.get("categoria") or {}
            cat_id = str(cat.get("id", "")) if isinstance(cat, dict) else ""
            cat_nome = cat.get("descricao") or cat.get("nome") or "" if isinstance(cat, dict) else ""
            contato = d.get("cliente") or d.get("contato") or {}
            cliente = contato.get("nome", "") if isinstance(contato, dict) else ""
            data_emi = d.get("dataEmissao") or d.get("data") or ""
            # Tiny pode devolver em DD/MM/AAAA — normaliza pra AAAA-MM-DD
            if "/" in data_emi:
                try:
                    dd, mm2, yy = data_emi.split("/")
                    data_emi = f"{yy}-{mm2.zfill(2)}-{dd.zfill(2)}"
                except Exception:
                    pass
            servico_norm = apply_alias(config, "servico", (cat_nome or "").strip().upper())
            row = {
                "id_tiny":      d.get("id", ""),
                "data":         data_emi,
                "cliente":      clean_text(cliente).upper(),
                "categoria_id": cat_id,
                "categoria":    cat_nome,
                "servico_norm": servico_norm,
                "valor":        float(d.get("valor", 0) or 0),
                "historico":    d.get("historico", ""),
                "fetched_at":   ts_now,
            }
            if _db_upsert_hist(unit, state_dir, row):
                novos += 1
            else:
                atualizados += 1

        return _json({
            "success": True,
            "unit": unit, "mes": mes,
            "total_api": len(todos),
            "detalhes_ok": len(detalhes),
            "novos": novos,
            "atualizados": atualizados,
            "total_tabela": _db_count_hist(unit, state_dir, mes),
        })
    except Exception as exc:
        app.logger.exception("[bi.sync-historico] falha")
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/gerencial/api/bi/debug-tiny/<unit>")
@master_required
def master_api_bi_debug_tiny(unit: str):
    """Diagnostico do token Tiny de uma unidade: JWT claims + teste GET contas-receber."""
    import base64
    try:
        if unit not in UNITS:
            return _json({"success": False, "error": "unit invalida"}, 400)

        state_dir = _unit_state_dir(unit)
        token_file = state_dir / "tiny_tokens.json"
        out: dict[str, Any] = {"unit": unit, "token_file_exists": token_file.exists()}

        if token_file.exists():
            try:
                stored = json.loads(token_file.read_text())
                at = stored.get("access_token", "")
                rt = stored.get("refresh_token", "")
                out["has_access_token"] = bool(at)
                out["has_refresh_token"] = bool(rt)
                out["access_token_preview"] = (at[:12] + "..." + at[-6:]) if at else ""
                out["refresh_token_tail"] = rt[-6:] if rt else ""
                out["expires_at"] = stored.get("expires_at")
                out["file_mtime"] = dt.datetime.fromtimestamp(token_file.stat().st_mtime).isoformat(timespec="seconds")

                if at and at.count(".") == 2:
                    try:
                        payload_b64 = at.split(".")[1]
                        pad = "=" * (-len(payload_b64) % 4)
                        payload_raw = base64.urlsafe_b64decode(payload_b64 + pad).decode("utf-8", errors="replace")
                        claims = json.loads(payload_raw)
                        out["jwt_claims"] = {
                            "scope": claims.get("scope"),
                            "scopes": claims.get("scopes"),
                            "realm_access": claims.get("realm_access"),
                            "resource_access": claims.get("resource_access"),
                            "aud": claims.get("aud"),
                            "azp": claims.get("azp"),
                            "iss": claims.get("iss"),
                            "exp": claims.get("exp"),
                            "iat": claims.get("iat"),
                            "sub": claims.get("sub"),
                            "preferred_username": claims.get("preferred_username"),
                        }
                    except Exception as exc_jwt:
                        out["jwt_decode_error"] = str(exc_jwt)
            except Exception as exc_read:
                out["token_read_error"] = str(exc_read)

        # Teste chamada contas-receber
        try:
            config   = _build_unit_config(unit)
            importer = TinyImporter(config, state_dir)
            import requests as _rq
            url = f"{importer.client.base_url}/contas-receber"
            headers = {"Authorization": f"Bearer {importer.client.access_token()}", "Accept": "application/json"}
            resp = _rq.get(url, headers=headers, params={"limit": 1}, timeout=15)
            out["test_call"] = {
                "url": url,
                "status": resp.status_code,
                "body_preview": resp.text[:800],
                "headers_www_authenticate": resp.headers.get("WWW-Authenticate", ""),
            }
        except Exception as exc_call:
            out["test_call_error"] = str(exc_call)

        return _json({"success": True, "debug": out})
    except Exception as exc:
        app.logger.exception("[bi.debug-tiny] falha")
        return _json({"success": False, "error": str(exc)}, 500)


_FP_LABELS = {
    "dinheiro": "Dinheiro",
    "debito":   "Cartão de débito",
    "credito":  "Cartão de crédito",
    "pix":      "Pix",
    "faturado": "Faturado",
    "avista":   "À vista",
    "detran":   "DETRAN",
}


def _norm_fp_bi(fp: str) -> str:
    f = (fp or "").strip().lower()
    if f == "fa": return "faturado"
    if f == "av": return "avista"
    return f


@app.route("/gerencial/api/bi/faturamento", methods=["GET"])
@master_view_required
def master_api_bi_faturamento():
    """Agrega envios_tiny do mes (o que o SISTEMA emitiu em fechamento de caixa).

    Params: mes=AAAA-MM (obrigatorio), unit=<slug> (opcional).
    Fonte unica: envios_tiny (populado a cada envio pro Tiny no fechamento).
    Nao usa historico_tiny/XLS — BI foca em EMITIDO, nao em recebido.
    """
    try:
        mes  = (request.args.get("mes") or "").strip()
        unit = (request.args.get("unit") or "").strip() or None
        if not mes or len(mes) != 7 or mes[4] != "-":
            return _json({"success": False, "error": "mes invalido (use AAAA-MM)"}, 400)
        try:
            ano, mm = mes.split("-")
            ano, mm = int(ano), int(mm)
            data_ini = dt.date(ano, mm, 1)
            data_fim = (data_ini.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1)
            date_from = data_ini.isoformat()
            date_to   = data_fim.isoformat()
        except Exception:
            return _json({"success": False, "error": "mes invalido (use AAAA-MM)"}, 400)

        units_iter = [unit] if unit and unit in UNITS else list(UNITS.keys())
        registros: list[dict] = []
        por_unit: dict[str, dict] = {}
        for uid in units_iter:
            state_dir = _unit_state_dir(uid)
            try:
                envios = _db_load_envios_range(uid, state_dir, date_from, date_to)
            except Exception as exc:
                app.logger.warning("[bi.faturamento] falha unit=%s: %s", uid, exc)
                envios = []
            lista = []
            for e in envios:
                servico_raw = (e.get("servico") or "").strip()
                lista.append({
                    "unit":         uid,
                    "unit_nome":    UNITS.get(uid, {}).get("nome", uid),
                    "data":         e.get("data_lancamento", ""),
                    "placa":        e.get("placa", ""),
                    "cliente":      (e.get("cliente") or "").strip().upper(),
                    "servico_norm": servico_raw.upper() or "(sem categoria)",
                    "valor":        float(e.get("valor", 0) or 0),
                    "fp":           _norm_fp_bi(e.get("fp", "")),
                })
            registros.extend(lista)
            sub_total = sum(r["valor"] for r in lista)
            por_unit[uid] = {
                "nome":  UNITS.get(uid, {}).get("nome", uid),
                "total": round(sub_total, 2),
                "count": len(lista),
            }

        total_geral = sum(r["valor"] for r in registros)
        count_total = len(registros)

        # Ranking por servico (categoria emitida)
        por_servico: dict[str, dict] = {}
        for r in registros:
            key = r["servico_norm"]
            b = por_servico.setdefault(key, {"servico": key, "count": 0, "total": 0.0})
            b["count"] += 1
            b["total"] += r["valor"]
        ranking_servicos = sorted(
            [{**v, "total": round(v["total"], 2)} for v in por_servico.values()],
            key=lambda x: x["total"], reverse=True,
        )

        # Ranking por cliente
        por_cliente: dict[str, dict] = {}
        for r in registros:
            key = r["cliente"] or "(sem cliente)"
            b = por_cliente.setdefault(key, {"cliente": key, "count": 0, "total": 0.0})
            b["count"] += 1
            b["total"] += r["valor"]
        ranking_clientes = sorted(
            [{**v, "total": round(v["total"], 2)} for v in por_cliente.values()],
            key=lambda x: x["total"], reverse=True,
        )

        # Por dia + por dia × categoria
        por_dia: dict[str, dict] = {}
        por_dia_cat: dict[str, dict] = {}
        for r in registros:
            data = r["data"]
            if not data:
                continue
            cat = r["servico_norm"]
            valor = r["valor"]
            d = por_dia.setdefault(data, {"data": data, "count": 0, "total": 0.0})
            d["count"] += 1
            d["total"] += valor
            dc = por_dia_cat.setdefault(data, {})
            c = dc.setdefault(cat, {"count": 0, "total": 0.0})
            c["count"] += 1
            c["total"] += valor
        faturamento_por_dia = sorted(
            [{**v, "total": round(v["total"], 2)} for v in por_dia.values()],
            key=lambda x: x["data"],
        )
        faturamento_por_dia_cat = sorted(
            [
                {
                    "data":  data,
                    "total": round(sum(c["total"] for c in cats.values()), 2),
                    "count": sum(c["count"] for c in cats.values()),
                    "categorias": sorted(
                        [{"categoria": cat, "count": c["count"], "total": round(c["total"], 2)}
                         for cat, c in cats.items()],
                        key=lambda x: x["total"], reverse=True,
                    ),
                }
                for data, cats in por_dia_cat.items()
            ],
            key=lambda x: x["data"],
        )

        # Por forma de pagamento (fp dos envios)
        por_forma_buckets: dict[str, dict] = {}
        for r in registros:
            fp = r["fp"] or "(sem forma)"
            label = _FP_LABELS.get(fp, fp.upper() if fp != "(sem forma)" else fp)
            b = por_forma_buckets.setdefault(label, {"forma": label, "count": 0, "total": 0.0})
            b["count"] += 1
            b["total"] += r["valor"]
        por_forma = sorted(
            [{**v, "total": round(v["total"], 2)} for v in por_forma_buckets.values()],
            key=lambda x: x["total"], reverse=True,
        )

        return _json({
            "success": True,
            "mes": mes,
            "total": round(total_geral, 2),
            "count": count_total,
            "por_unit": por_unit,
            "ranking_servicos": ranking_servicos,
            "ranking_clientes": ranking_clientes,
            "por_dia": faturamento_por_dia,
            "por_dia_categoria": faturamento_por_dia_cat,
            "por_forma": por_forma,
        })
    except Exception as exc:
        app.logger.exception("[bi.faturamento] falha")
        return _json({"success": False, "error": str(exc)}, 500)


def _classificar_categoria_receita(servico_norm: str) -> str:
    """Agrupa servico_norm em 3 baldes pedidos por Ian.

    - 'transferencia' : contem TRANSFER
    - 'cautelar_pintura' : cautelar + analise/verificacao/pintura
    - 'cautelar' : cautelar puro
    - ''         : outros (nao se encaixa nos 3)
    """
    up = (servico_norm or "").strip().upper()
    if not up:
        return ""
    if "TRANSFER" in up:
        return "transferencia"
    if "CAUTELAR" in up and ("ANALISE" in up or "VERIFICACAO" in up or "PINTURA" in up or "COM " in up):
        return "cautelar_pintura"
    if "CAUTELAR" in up:
        return "cautelar"
    return ""


@app.route("/gerencial/api/bi/historico-emitido", methods=["GET"])
@master_view_required
def master_api_bi_historico_emitido():
    """Agrega historico_tiny (contas a receber emitidas no Tiny) do mes.

    Params: mes=AAAA-MM (obrigatorio), unit=<slug> (opcional).
    Fonte: historico_tiny — snapshot do que foi emitido e gravado via sync do Tiny.
    Complementa o BI principal (que usa envios_tiny), preservando o historico
    anterior a 22/04/2026 (quando envios_tiny comecou a acumular por conta propria).
    """
    try:
        mes  = (request.args.get("mes") or "").strip()
        unit = (request.args.get("unit") or "").strip() or None
        if not mes or len(mes) != 7 or mes[4] != "-":
            return _json({"success": False, "error": "mes invalido (use AAAA-MM)"}, 400)

        units_iter = [unit] if unit and unit in UNITS else list(UNITS.keys())
        registros: list[dict] = []
        por_unit: dict[str, dict] = {}
        for uid in units_iter:
            state_dir = _unit_state_dir(uid)
            try:
                rows = _db_load_hist_mes(uid, state_dir, mes)
            except Exception as exc:
                app.logger.warning("[bi.historico-emitido] falha unit=%s: %s", uid, exc)
                rows = []
            lista = []
            for r in rows:
                servico = (r.get("servico_norm") or r.get("categoria") or "").strip().upper() or "(sem categoria)"
                if not _e_categoria_de_servico(servico):
                    continue
                lista.append({
                    "unit":    uid,
                    "data":    r.get("data", ""),
                    "cliente": (r.get("cliente") or "").strip().upper() or "(sem cliente)",
                    "servico": servico,
                    "valor":   float(r.get("valor", 0) or 0),
                })
            registros.extend(lista)
            por_unit[uid] = {
                "nome":  UNITS.get(uid, {}).get("nome", uid),
                "total": round(sum(r["valor"] for r in lista), 2),
                "count": len(lista),
            }

        total_bruto = round(sum(r["valor"] for r in registros), 2)
        count_laudos = len(registros)

        # Top categorias (por receita)
        por_cat: dict[str, dict] = {}
        for r in registros:
            b = por_cat.setdefault(r["servico"], {"categoria": r["servico"], "count": 0, "total": 0.0})
            b["count"] += 1
            b["total"] += r["valor"]
        ranking_categorias = sorted(
            [{**v, "total": round(v["total"], 2)} for v in por_cat.values()],
            key=lambda x: x["total"], reverse=True,
        )
        top3_categorias = ranking_categorias[:3]

        # Baldes fixos: transferencia / cautelar / cautelar com pintura
        baldes = {
            "transferencia":    {"label": "Laudo de transferência",    "count": 0, "total": 0.0},
            "cautelar":         {"label": "Vistoria cautelar",         "count": 0, "total": 0.0},
            "cautelar_pintura": {"label": "Cautelar com pintura",      "count": 0, "total": 0.0},
        }
        for r in registros:
            b = _classificar_categoria_receita(r["servico"])
            if b and b in baldes:
                baldes[b]["count"] += 1
                baldes[b]["total"] += r["valor"]
        categorias_fixas = [
            {"key": k, **v, "total": round(v["total"], 2)}
            for k, v in baldes.items()
        ]

        # Top 10 clientes com breakdown por categoria
        por_cliente: dict[str, dict] = {}
        for r in registros:
            c = por_cliente.setdefault(r["cliente"], {
                "cliente": r["cliente"], "count": 0, "total": 0.0, "por_categoria": {},
            })
            c["count"] += 1
            c["total"] += r["valor"]
            cat = c["por_categoria"].setdefault(r["servico"], {"count": 0, "total": 0.0})
            cat["count"] += 1
            cat["total"] += r["valor"]
        top10_clientes = sorted(
            por_cliente.values(), key=lambda x: x["total"], reverse=True,
        )[:10]
        top10_clientes = [
            {
                "cliente": c["cliente"],
                "count":   c["count"],
                "total":   round(c["total"], 2),
                "categorias": sorted(
                    [{"categoria": cat, "count": v["count"], "total": round(v["total"], 2)}
                     for cat, v in c["por_categoria"].items()],
                    key=lambda x: x["total"], reverse=True,
                ),
            }
            for c in top10_clientes
        ]

        return _json({
            "success": True,
            "mes": mes,
            "total_bruto":   total_bruto,
            "count_laudos":  count_laudos,
            "por_unit":      por_unit,
            "top3_categorias":   top3_categorias,
            "categorias_fixas":  categorias_fixas,
            "top10_clientes":    top10_clientes,
            "ranking_categorias": ranking_categorias,
        })
    except Exception as exc:
        app.logger.exception("[bi.historico-emitido] falha")
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/gerencial/api/backup/download")
@master_required
def master_api_backup_download():
    """Baixa um zip com dump SQL de cada unidade + configs. Download direto no navegador.

    Reusa _criar_backup_zip() que ja existe — usa iterdump() do SQLite, portanto
    o dump e consistente mesmo com escrita concorrente (snapshot SQL atomico).
    Complementa o backup automatico via email que roda todo dia 00:00.
    """
    import io
    try:
        zip_bytes = _criar_backup_zip()
    except Exception as exc:
        app.logger.exception("[backup.download] falha ao gerar zip")
        return _json({"error": f"falha ao gerar backup: {exc}"}, 500)

    stamp = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y%m%d-%H%M%S")
    filename = f"astro-backup-{stamp}.zip"
    app.logger.info("[backup.download] size_kb=%s filename=%s", len(zip_bytes) // 1024, filename)
    return send_file(io.BytesIO(zip_bytes), mimetype="application/zip",
                     as_attachment=True, download_name=filename)


@app.route("/master/api/maintenance", methods=["GET"])
@master_required
def master_api_maintenance_status():
    info = {"active": _is_maintenance()}
    if info["active"]:
        try:
            info["detalhe"] = json.loads(_MAINTENANCE_FLAG.read_text(encoding="utf-8"))
        except Exception:
            info["detalhe"] = None
    return _json(info)


@app.route("/master/api/maintenance/on", methods=["POST"])
@master_required
@csrf_required
def master_api_maintenance_on():
    user = _current_user() or {}
    _set_maintenance(True, who=user.get("email", ""))
    app.logger.info("[maintenance] ON by %s", user.get("email"))
    return _json({"success": True, "active": True})


@app.route("/master/api/maintenance/off", methods=["POST"])
@master_required
@csrf_required
def master_api_maintenance_off():
    user = _current_user() or {}
    _set_maintenance(False)
    app.logger.info("[maintenance] OFF by %s", user.get("email"))
    return _json({"success": True, "active": False})


@app.route("/master/api/debug/email-test", methods=["GET"])
@master_required
def master_api_debug_email_test():
    """Envia email de teste. Uso: ?to=email@dominio.com[&provider=resend|smtp][&host=&port=&mode=ssl|starttls].

    Sem ?provider: auto-seleciona (resend se RESEND_API_KEY setada, senao smtp).
    Com ?provider=resend|smtp: forca aquele provider.
    Params host/port/mode so afetam smtp.

    Retorna JSON com diagnostico completo (config, conexao, envio).
    """
    import smtplib
    from email.mime.text import MIMEText
    import base64, json as _json_mod, urllib.request, urllib.error

    to_override       = (request.args.get("to") or "").strip()
    provider_override = (request.args.get("provider") or "").strip().lower()
    host_override     = (request.args.get("host") or "").strip()
    port_override     = (request.args.get("port") or "").strip()
    mode_override     = (request.args.get("mode") or "").strip().lower()

    provider = provider_override if provider_override in ("resend", "smtp") else (_email_provider() or "smtp")
    alert_emails_raw = os.environ.get("ALERT_EMAILS", "")
    alert_emails = [e.strip() for e in alert_emails_raw.split(",") if e.strip()]
    recipients = [to_override] if to_override else alert_emails

    agora = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds")
    subject = f"[Astrovistorias] Teste Email {agora}"
    html = f"<p>Teste de email disparado em <b>{agora}</b>.</p><p>Se voce recebeu esse email, o envio esta funcionando.</p>"

    diag = {
        "provider": provider,
        "provider_override": provider_override or None,
        "config": {
            "ALERT_EMAILS_raw": alert_emails_raw,
            "ALERT_EMAILS_parsed": alert_emails,
            "recipients_used": recipients,
            "to_override": to_override or None,
        },
    }

    if not recipients:
        diag["result"] = "FALTA_DESTINATARIO"
        diag["hint"] = "ALERT_EMAILS env vazia e nenhum ?to= passado. Passe ?to=email@dominio.com ou configure ALERT_EMAILS."
        return _json(diag, 400)

    # ─── RESEND ───────────────────────────────────────────────
    if provider == "resend":
        api_key    = os.environ.get("RESEND_API_KEY", "").strip()
        from_email = os.environ.get("RESEND_FROM", "").strip() or "Astrovistorias <onboarding@resend.dev>"
        diag["config"]["RESEND_API_KEY_set"] = bool(api_key)
        diag["config"]["RESEND_API_KEY_len"] = len(api_key)
        diag["config"]["RESEND_FROM"] = from_email
        if not api_key:
            diag["result"] = "FALTA_CONFIG"
            diag["hint"] = "RESEND_API_KEY vazia. Seta no Railway > Variables. Pega a key em resend.com > API Keys."
            return _json(diag, 400)
        payload = {"from": from_email, "to": recipients, "subject": subject, "html": html}
        req = urllib.request.Request(
            "https://api.resend.com/emails",
            data=_json_mod.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type":  "application/json",
                "User-Agent":    "AstroVistorias/1.0 (+https://astrovistorias.com.br)",
                "Accept":        "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                body = resp.read().decode("utf-8", errors="replace")
            diag["result"] = "ENVIADO"
            diag["message"] = f"Email enviado via Resend para {recipients} (from={from_email}). Verifique caixa de entrada e spam."
            diag["resend_response"] = body[:500]
            app.logger.info("[email-test:resend] ok to=%s", recipients)
            return _json(diag)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            diag["result"] = "RESEND_HTTP_ERROR"
            diag["error"] = f"HTTP {exc.code}: {body[:500]}"
            diag["hint"] = "Erro da API Resend. 401 = chave invalida. 403 = dominio nao verificado (use onboarding@resend.dev). 422 = payload invalido."
            return _json(diag, 500)
        except Exception as exc:
            diag["result"] = "OTHER_ERROR"
            diag["error"] = f"{type(exc).__name__}: {exc}"
            return _json(diag, 500)

    # ─── SMTP ─────────────────────────────────────────────────
    host   = host_override or os.environ.get("SMTP_HOST", "")
    port   = int(port_override) if port_override.isdigit() else int(os.environ.get("SMTP_PORT", "465") or 465)
    mode   = mode_override if mode_override in ("ssl", "starttls") else ("starttls" if port == 587 else "ssl")
    user   = os.environ.get("SMTP_USER", "")
    passwd = os.environ.get("SMTP_PASS", "")

    diag["config"].update({
        "SMTP_HOST": host or "(vazio)",
        "SMTP_PORT": port,
        "SMTP_MODE": mode,
        "SMTP_USER": user or "(vazio)",
        "SMTP_PASS_len": len(passwd),
        "host_override": host_override or None,
        "port_override": port_override or None,
        "mode_override": mode_override or None,
    })
    diag["checks"] = {
        "host_ok": bool(host),
        "user_ok": bool(user),
        "password_ok": bool(passwd),
        "recipients_ok": bool(recipients),
    }
    if not all([host, user, passwd]):
        diag["result"] = "FALTA_CONFIG"
        diag["hint"] = "SMTP_HOST/USER/PASS incompletos. Se for migrar para Resend, seta RESEND_API_KEY no Railway."
        return _json(diag, 400)

    msg = MIMEText(html, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"]    = f"Astrovistorias <{user}>"
    msg["To"]      = ", ".join(recipients)

    try:
        if mode == "starttls":
            with smtplib.SMTP(host, port, timeout=20) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.ehlo()
                smtp.login(user, passwd)
                smtp.sendmail(user, recipients, msg.as_string())
        else:
            with smtplib.SMTP_SSL(host, port, timeout=20) as smtp:
                smtp.login(user, passwd)
                smtp.sendmail(user, recipients, msg.as_string())
        diag["result"] = "ENVIADO"
        diag["message"] = f"Email enviado via SMTP para {recipients} via {host}:{port} ({mode}). Verifique caixa de entrada e spam."
        app.logger.info("[email-test:smtp] ok to=%s via=%s:%s/%s", recipients, host, port, mode)
        return _json(diag)
    except smtplib.SMTPAuthenticationError as exc:
        diag["result"] = "AUTH_ERROR"
        diag["error"] = str(exc)
        diag["hint"] = "Usuario ou senha SMTP invalidos."
        return _json(diag, 500)
    except (smtplib.SMTPConnectError, OSError) as exc:
        diag["result"] = "CONNECTION_ERROR"
        diag["error"] = f"{type(exc).__name__}: {exc}"
        diag["hint"] = "Falha ao conectar. Railway pode bloquear SMTP outbound — migre para Resend (RESEND_API_KEY)."
        return _json(diag, 500)
    except Exception as exc:
        diag["result"] = "OTHER_ERROR"
        diag["error"] = f"{type(exc).__name__}: {exc}"
        return _json(diag, 500)


def _human_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    for unit in ("KB", "MB", "GB", "TB"):
        n /= 1024.0
        if n < 1024:
            return f"{n:.2f} {unit}"
    return f"{n:.2f} PB"


# ══════════════════════════════════════════════════════════════════════════════
# Rotas: arquivos estaticos da frente de caixa
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/u/<unit>/")
@unit_access_required
def unit_index(unit: str):
    return redirect(f"/u/{unit}/caixa2")


@app.route("/u/<unit>/<path:filename>")
@unit_access_required
def unit_static(unit: str, filename: str):
    resp = send_from_directory(UI_DIR, filename)
    if filename.endswith((".js", ".html", ".css")):
        resp.headers["Cache-Control"] = "no-store"
    return resp


# ══════════════════════════════════════════════════════════════════════════════
# Rotas: API da unidade e OAuth
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/u/<unit>/auth")
@unit_access_required
def api_auth_start(unit: str):
    """Inicia o fluxo OAuth para a unidade."""
    try:
        config = _build_unit_config(unit)
        tiny = config["tiny"]
        redirect_uri = tiny.get("redirect_uri") or f"https://{request.host}/u/{unit}/callback"
        params = {
            "client_id": tiny["client_id"],
            "redirect_uri": redirect_uri,
            "scope": tiny["oauth_scope"],
            "response_type": "code",
        }
        url = f"{tiny['auth_url']}?{urllib.parse.urlencode(params)}"
        return redirect(url)
    except KeyError as exc:
        return f"<h1>Configuracao Tiny incompleta</h1><p>Campo ausente: <code>{exc}</code> para unidade <strong>{unit}</strong>.</p><p>Verifique UNITS_CONFIG no Railway.</p>", 500
    except Exception as exc:
        app.logger.exception("[server] auth unit=%s", unit)
        return f"<h1>Erro ao iniciar autenticacao</h1><pre>{exc}</pre>", 500


@app.route("/u/<unit>/callback")
def api_auth_callback(unit: str):
    """Recebe o code do Tiny e troca pelo refresh_token.
    O token e salvo direto no arquivo da unidade — zero intervencao no Railway."""
    code = request.args.get("code")
    if not code:
        return Response("Code ausente", status=400)

    config = _build_unit_config(unit)
    state_dir = _unit_state_dir(unit)
    importer = TinyImporter(config, state_dir)

    redirect_uri = config["tiny"].get("redirect_uri") or f"https://{request.host}/u/{unit}/callback"

    try:
        app.logger.info("[oauth.callback] unit=%s ok=start redirect_uri=%s", unit, redirect_uri)
        importer.client.exchange_authorization_code(code, redirect_uri)
        app.logger.info("[oauth.callback] unit=%s ok=True token_saved", unit)
        return (
            f"<!doctype html><meta charset='utf-8'>"
            f"<title>Tiny autorizado — {unit}</title>"
            f"<style>body{{font-family:system-ui,sans-serif;max-width:640px;margin:80px auto;padding:0 24px;color:#222}}"
            f".ok{{background:#d4f7dc;border:1px solid #2fa84f;border-radius:8px;padding:20px;margin:24px 0}}"
            f"a.btn{{display:inline-block;background:#2b60d9;color:#fff;padding:12px 24px;border-radius:6px;text-decoration:none;margin-top:16px}}"
            f"a.btn:hover{{background:#1f4ab0}}</style>"
            f"<h1>Tiny autorizado com sucesso</h1>"
            f"<div class='ok'><strong>Unidade:</strong> {unit}<br>"
            f"O token foi salvo e a unidade ja pode enviar para o Tiny.</div>"
            f"<a class='btn' href='/u/{unit}/home'>Voltar para a unidade</a>"
        )
    except Exception as exc:
        app.logger.exception("[oauth.callback] unit=%s ok=False", unit)
        return (
            f"<!doctype html><meta charset='utf-8'>"
            f"<title>Erro ao autorizar — {unit}</title>"
            f"<style>body{{font-family:system-ui,sans-serif;max-width:640px;margin:80px auto;padding:0 24px;color:#222}}"
            f".err{{background:#fde0e0;border:1px solid #c62828;border-radius:8px;padding:20px;margin:24px 0}}"
            f"pre{{background:#f4f4f4;padding:12px;border-radius:4px;overflow:auto;font-size:12px}}"
            f"a.btn{{display:inline-block;background:#2b60d9;color:#fff;padding:12px 24px;border-radius:6px;text-decoration:none;margin-top:16px}}</style>"
            f"<h1>Falha na autorizacao</h1>"
            f"<div class='err'><pre>{exc}</pre></div>"
            f"<a class='btn' href='/u/{unit}/auth'>Tentar novamente</a>"
        )

@app.route("/u/<unit>/api/info")
@unit_access_required
def api_info(unit: str):
    ud = UNITS.get(unit, {})
    user = _current_user()
    # Deriva lista de servicos dos categoria_ids configurados
    # servicos_pdv tem prioridade; se nao definido, usa chaves de categoria_ids; se vazio, usa fallback
    servicos_pdv = ud.get("servicos_pdv")
    if servicos_pdv:
        servicos = servicos_pdv
    else:
        unit_cats = _load_unit_categorias(unit)
        if unit_cats:
            servicos = list(unit_cats.keys())
        else:
            categoria_ids = ud.get("categoria_ids", {})
            servicos = list(categoria_ids.keys()) if categoria_ids else [
                "LAUDO DE TRANSFERENCIA",
                "LAUDO CAUTELAR",
                "CAUTELAR COM ANALISE DE PINTURA",
                "REVISTORIA",
                "BAIXA PERMANENTE",
                "CONSULTA GRAVAME",
                "EMISSAO CRLV",
                "PESQUISA AVULSA",
                "VISTORIA ESTRUTURAL SEM EMISSAO DE LAUDO",
            ]
    return _json({
        "unidade": ud.get("nome", unit),
        "usuario": session.get("name", ""),
        "email": session.get("email", ""),
        "master": bool(user and user.get("master")),
        "matriz": bool(user and user.get("matriz")),
        "gerencial": bool(user and (user.get("gerencial") or user.get("master") or user.get("matriz"))),
        "servicos": servicos,
        "pin_configurado": bool(ud.get("master_pin")),
    })


# Mapeamento de IDs de forma de recebimento → nome legivel
_FORMA_NAMES: dict[int, str] = {
    556498207: "Dinheiro",
    556498209: "Cartao de credito",
    556498211: "Cartao de debito",
    556498213: "Boleto",
    556498217: "Deposito",
    598163085: "Dinheiro",
    598163087: "Cartao de credito",
    598163089: "Cartao de debito",
    598163095: "Deposito",
    702313264: "A faturar",
    802165201: "A faturar",
    802165265: "Cortesia",
    803887338: "Retorno",
}


@app.route("/u/<unit>/api/preview", methods=["POST"])
@unit_access_required
@csrf_required
def api_preview(unit: str):
    try:
        data       = request.get_json(force=True, silent=True) or {}
        config     = _build_unit_config(unit)
        state_dir  = _unit_state_dir(unit)
        tiny_config = config["tiny"]
        forma_ids   = tiny_config.get("forma_recebimento_ids", {})

        state_path = state_dir / "imported.json"
        imported   = load_state(state_path).get("imported", {})

        previews = []
        for r in data.get("records", []):
            chave  = r.get("id", "?")
            av_pag = r.get("avPagamento", "")
            fp     = r.get("fp", "")
            av     = is_av_paid(av_pag)

            servico_raw = clean_text(r.get("servico", "")).upper()
            servico = apply_alias(config, "servico", servico_raw)
            rec = NormalizedRecord(
                data=r["data"], modelo=r.get("modelo", ""),
                placa=r.get("placa", ""), cliente=r.get("cliente", ""),
                servico=servico, fp=fp,
                preco=str(r.get("preco", "0")),
                origem_arquivo=r.get("origemArquivo", "manual_ui"),
                linha_origem=r.get("linhaOrigem", 0),
                chave_deduplicacao=chave, av_pagamento=av_pag,
                cpf=r.get("cpf", ""),
            )

            pay_key = av_pag if av else fp
            pay_id  = lookup_config_id(forma_ids, pay_key)
            due     = rec.data if av else last_day_of_month(rec.data)
            num_doc = compact_document_number(tiny_config, rec)
            forma_display = (
                f"{_FORMA_NAMES.get(pay_id, str(pay_id))} (ID {pay_id})"
                if pay_id else "nao mapeado"
            )

            payload: dict[str, Any] = {
                "data": rec.data,
                "dataVencimento": due,
                "dataCompetencia": rec.data[:7],
                "valor": money_as_float(rec.preco),
                "contato": {"nome": rec.cliente},
                "numeroDocumento": num_doc,
                "historico": build_history(rec),
                "ocorrencia": "U",
            }
            if pay_id and tiny_config.get("include_forma_recebimento"):
                payload["formaRecebimento"] = pay_id
            if cat := resolve_categoria_id(tiny_config, rec.servico):
                payload["categoria"] = {"id": cat}

            previews.append({
                "chave": chave, "cliente": rec.cliente,
                "fp": fp, "avPagamento": av_pag,
                "valor": money_as_float(rec.preco),
                "dataVencimento": due,
                "formaRecebimento": forma_display,
                "numeroDocumento": num_doc,
                "jaEnviado": chave in imported,
                "servico": rec.servico,
                "payload": payload,
            })

        novos = sum(1 for p in previews if not p["jaEnviado"])
        dups  = sum(1 for p in previews if p["jaEnviado"])
        return _json({
            "success": True, "previews": previews,
            "resumo": {"novos": novos, "duplicatas": dups, "total": len(previews)},
        })
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/send", methods=["POST"])
@unit_access_required
@csrf_required
def api_send(unit: str):
    try:
        data      = request.get_json(force=True, silent=True) or {}
        config    = _build_unit_config(unit)
        state_dir = _unit_state_dir(unit)
        _seed_tokens(unit, config)

        state_path = state_dir / "imported.json"
        st         = load_state(state_path)
        imported   = st.setdefault("imported", {})
        importer   = TinyImporter(config, state_dir)
        results: dict[str, list] = {"enviados": [], "pulados": [], "falhas": []}

        # Lock protege imported dict e results de acessos concorrentes entre threads
        lock = threading.Lock()

        def _process_one(r: dict) -> None:
            """Processa um unico registro. Executado em thread pool."""
            servico_raw = clean_text(r.get("servico", "")).upper()
            servico = apply_alias(config, "servico", servico_raw)
            rec = NormalizedRecord(
                data=r["data"], modelo=r.get("modelo", ""),
                placa=r.get("placa", ""), cliente=r["cliente"],
                servico=servico, fp=r["fp"],
                preco=str(r["preco"]),
                origem_arquivo=r.get("origemArquivo", "manual_ui"),
                linha_origem=r.get("linhaOrigem", 0),
                chave_deduplicacao=r.get("id", "missing_key"),
                av_pagamento=r.get("avPagamento", ""),
                cpf=r.get("cpf", ""),
            )
            if rec.chave_deduplicacao == "missing_key" or "-" in rec.chave_deduplicacao:
                rec.chave_deduplicacao = record_key(asdict(rec))

            # Camada 1: check local (thread-safe via lock)
            with lock:
                if rec.chave_deduplicacao in imported:
                    results["pulados"].append({"chave": rec.chave_deduplicacao, "cliente": rec.cliente, "motivo": "ja importado", "record": r})
                    return

            try:
                # Retry com backoff: 3 tentativas, espera 2s e 4s entre elas
                # Cobre quedas temporárias do Tiny sem perder o lançamento
                last_exc: Exception | None = None
                resp = None
                for attempt in range(3):
                    try:
                        resp = importer.create_accounts_receivable(rec)
                        break
                    except Exception as exc:
                        if _is_doc_already_registered(exc):
                            raise  # não retenta duplicata — vai direto para o handler abaixo
                        last_exc = exc
                        if attempt < 2:
                            time.sleep(2 ** attempt)  # 0s, 2s, 4s
                else:
                    raise last_exc  # esgotou tentativas
                with lock:
                    imported[rec.chave_deduplicacao] = {
                        "arquivo": rec.origem_arquivo,
                        "linha": rec.linha_origem,
                        "enviado_em": dt.datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds"),
                        "resposta": resp,
                    }
                    results["enviados"].append({"chave": rec.chave_deduplicacao, "cliente": rec.cliente, "record": r})
            except Exception as exc:
                with lock:
                    if _is_doc_already_registered(exc):
                        imported[rec.chave_deduplicacao] = {
                            "arquivo": rec.origem_arquivo,
                            "linha": rec.linha_origem,
                            "enviado_em": dt.datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds"),
                            "motivo": "ja existia no Tiny (numeroDocumento duplicado)",
                        }
                        results["pulados"].append({"chave": rec.chave_deduplicacao, "cliente": rec.cliente, "motivo": "ja existia no Tiny", "record": r})
                    else:
                        app.logger.exception("[send] falha chave=%s cliente=%s", rec.chave_deduplicacao, rec.cliente)
                        results["falhas"].append({
                            "chave": rec.chave_deduplicacao,
                            "cliente": rec.cliente,
                            "erro": str(exc),
                            "record": r,
                        })

        # Processa todos os registros do lote em paralelo (5 threads concorrentes)
        # Cada thread faz chamadas de I/O ao Tiny de forma independente
        from concurrent.futures import ThreadPoolExecutor
        records = data.get("records", [])
        with ThreadPoolExecutor(max_workers=5) as pool:
            list(pool.map(_process_one, records))

        # Salva estado uma unica vez apos processar todos (escrita atomica)
        save_state(state_path, st)

        # Modo espelho: grava na tabela envios_tiny em paralelo ao imported.json.
        # Cada entry em results ja carrega o 'record' original (nao depende de lookup
        # por id, que quebrava quando a chave era recalculada via record_key).
        try:
            ts_now = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds")

            def _fp_normalizado(r: dict) -> str:
                """Resolve fp pro gerencial: FA/faturado→faturado, AV→avPagamento (dinheiro/pix/...)."""
                fp_raw = (r.get("fp", "") or "").strip()
                av_pag = (r.get("avPagamento", "") or "").strip().lower()
                if fp_raw.upper() == "FA":
                    return "faturado"
                if fp_raw.upper() == "AV":
                    return av_pag if av_pag and av_pag != "pendente" else "avista"
                return fp_raw.lower()

            for e in results["enviados"]:
                chave = e["chave"]
                r = e.get("record") or {}
                _db_insert_envio(unit, state_dir, {
                    "chave_deduplicacao": chave,
                    "timestamp":          ts_now,
                    "data_lancamento":    r.get("data", "") or "",
                    "placa":              r.get("placa", ""),
                    "cliente":            r.get("cliente", e.get("cliente", "")),
                    "servico":            r.get("servico", ""),
                    "valor":              float(r.get("preco", 0) or 0),
                    "fp":                 _fp_normalizado(r),
                    "status":             "enviado",
                    "arquivo":            r.get("origemArquivo", "manual_ui"),
                    "linha":              int(r.get("linhaOrigem", 0) or 0),
                    "resposta_tiny":      imported.get(chave, {}).get("resposta"),
                })
            for p in results["pulados"]:
                chave = p["chave"]
                r = p.get("record") or {}
                motivo = p.get("motivo", "")
                status = "ja_existia_tiny" if "existia no Tiny" in motivo else "ja_importado_local"
                _db_insert_envio(unit, state_dir, {
                    "chave_deduplicacao": chave,
                    "timestamp":          ts_now,
                    "data_lancamento":    r.get("data", "") or "",
                    "placa":              r.get("placa", ""),
                    "cliente":            r.get("cliente", p.get("cliente", "")),
                    "servico":            r.get("servico", ""),
                    "valor":              float(r.get("preco", 0) or 0),
                    "fp":                 _fp_normalizado(r),
                    "status":             status,
                    "arquivo":            r.get("origemArquivo", "manual_ui"),
                    "linha":              int(r.get("linhaOrigem", 0) or 0),
                    "erro":               motivo,
                })
            for f in results["falhas"]:
                chave = f["chave"]
                r = f.get("record") or {}
                _db_insert_envio(unit, state_dir, {
                    "chave_deduplicacao": chave,
                    "timestamp":          ts_now,
                    "data_lancamento":    r.get("data", "") or "",
                    "placa":              r.get("placa", ""),
                    "cliente":            r.get("cliente", f.get("cliente", "")),
                    "servico":            r.get("servico", ""),
                    "valor":              float(r.get("preco", 0) or 0),
                    "fp":                 _fp_normalizado(r),
                    "status":             "falha",
                    "arquivo":            r.get("origemArquivo", "manual_ui"),
                    "linha":              int(r.get("linhaOrigem", 0) or 0),
                    "erro":               f.get("erro", ""),
                })
        except Exception as mirror_exc:
            app.logger.warning("[envios_tiny:mirror] falha ao gravar tabela: %s", mirror_exc)

        # Email de confirmacao do envio (falha silenciosa — nao trava a resposta)
        try:
            _enviar_email_envio_tiny(unit, results, records)
        except Exception as email_exc:
            app.logger.warning("[envio_tiny:email] falha ao enviar email: %s", email_exc)

        # Trava automatica do caixa do dia quando o lote nao teve falha. Permite
        # reenviar partes (com o botao Limpar historico) enquanto tem pendencia,
        # e so fecha quando tudo rodou limpo. Pulados contam como sucesso (ja
        # tinham sido enviados antes).
        fechamento_auto = None
        if not results["falhas"]:
            user = _current_user() or {}
            user_email = session.get("email", "") or user.get("email", "")
            hoje_iso = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).date().isoformat()
            fechamento_auto = _fechar_dia(unit, hoje_iso, user_email, motivo="envio_tiny")

        return _json({
            "success": True, "summary": results,
            "message": f"Processamento concluido. Enviados: {len(results['enviados'])}",
            "fechamento": fechamento_auto,
        })
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/clear-imported", methods=["POST"])
@unit_access_required
@csrf_required
def api_clear_imported(unit: str):
    """Limpa o estado local de importacao (imported.json) para permitir reenvio."""
    try:
        state_dir  = _unit_state_dir(unit)
        state_path = state_dir / "imported.json"
        if state_path.exists():
            st = load_state(state_path)
            count = len(st.get("imported", {}))
            st["imported"] = {}
            save_state(state_path, st)
        else:
            count = 0
        return _json({"success": True, "message": f"Estado limpo. {count} registro(s) removidos."})
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/suggest-clients", methods=["POST"])
@unit_access_required
@csrf_required
def api_suggest_clients(unit: str):
    try:
        data = request.get_json(force=True, silent=True) or {}
        nome = clean_text(data.get("nome", ""))
        if not nome:
            raise ValueError("nome obrigatorio")

        config    = _build_unit_config(unit)
        state_dir = _unit_state_dir(unit)
        _seed_tokens(unit, config)

        importer = TinyImporter(config, state_dir)
        result   = importer.client.request("GET", "contatos", params={"nome": nome, "limit": 20})
        candidates = []

        for item in result.get("itens", []):
            item_nome     = item.get("nome", "")
            item_fantasia = item.get("fantasia", "") or ""
            score = max(
                similarity_score(nome, item_nome),
                similarity_score(nome, item_fantasia) if item_fantasia else 0.0,
            )
            candidates.append({
                "id": item.get("id"),
                "nome": item_nome,
                "fantasia": item_fantasia,
                "score": round(score, 2),
            })

        if len(candidates) < 3:
            result2 = importer.client.request("GET", "contatos", params={"limit": 100})
            seen = {c["id"] for c in candidates}
            for item in result2.get("itens", []):
                if item.get("id") in seen:
                    continue
                item_nome     = item.get("nome", "")
                item_fantasia = item.get("fantasia", "") or ""
                score = max(
                    similarity_score(nome, item_nome),
                    similarity_score(nome, item_fantasia) if item_fantasia else 0.0,
                )
                if score >= 0.2:
                    candidates.append({
                        "id": item.get("id"),
                        "nome": item_nome,
                        "fantasia": item_fantasia,
                        "score": round(score, 2),
                    })

        candidates.sort(key=lambda x: x["score"], reverse=True)
        return _json({"success": True, "candidates": candidates[:6]})
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/diagnostic-payment")
@unit_access_required
def api_diagnostic_payment(unit: str):
    """Retorna as formas de recebimento cadastradas no Tiny para diagnostico."""
    try:
        config = _build_unit_config(unit)
        state_dir = _unit_state_dir(unit)
        importer = TinyImporter(config, state_dir)
        # Tenta buscar do Tiny
        res = importer.client.request("GET", "formas-recebimento")
        return _json({
            "success": True,
            "tiny_response": res,
            "current_config": config["tiny"].get("forma_recebimento_ids")
        })
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/diagnostic-categorias")
@unit_access_required
def api_diagnostic_categorias(unit: str):
    """Retorna as categorias financeiras cadastradas no Tiny para diagnostico."""
    try:
        config = _build_unit_config(unit)
        state_dir = _unit_state_dir(unit)
        importer = TinyImporter(config, state_dir)
        res = importer.client.request("GET", "categorias-receita-despesa", params={"limit": 100})
        return _json({
            "success": True,
            "tiny_response": res,
            "current_config": config["tiny"].get("categoria_ids")
        })
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/map-client", methods=["POST"])
@unit_access_required
@csrf_required
def api_map_client(unit: str):
    try:
        data         = request.get_json(force=True, silent=True) or {}
        cliente_nome = clean_text(data.get("clienteNome", ""))
        tiny_id      = data.get("tinyId")
        if not cliente_nome or not tiny_id:
            raise ValueError("clienteNome e tinyId obrigatorios")

        ids = _load_extra_cliente_ids(unit)
        ids[cliente_nome] = int(tiny_id)
        _save_extra_cliente_ids(unit, ids)
        return _json({"success": True, "saved": True})
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/auto-map-clients", methods=["POST"])
@unit_access_required
@csrf_required
def api_auto_map_clients(unit: str):
    try:
        data      = request.get_json(force=True, silent=True) or {}
        clientes: list[str] = data.get("clientes", [])
        threshold: float    = float(data.get("threshold", 0.90))
        if not clientes:
            raise ValueError("clientes obrigatorio")

        config    = _build_unit_config(unit)
        state_dir = _unit_state_dir(unit)
        _seed_tokens(unit, config)
        importer  = TinyImporter(config, state_dir)

        # Carrega todos os contatos do Tiny de uma vez
        all_contacts: list[dict] = []
        page = 1
        while True:
            result = importer.client.request("GET", "contatos", params={"limit": 100, "offset": (page - 1) * 100})
            items  = result.get("itens", [])
            if not items:
                break
            all_contacts.extend(items)
            if len(items) < 100:
                break
            page += 1
            time.sleep(0.5) # Throttling para evitar 503/429 no Tiny


        mapped       = []
        needs_review = []
        # Carrega IDs salvos uma unica vez antes do loop
        ids = _load_extra_cliente_ids(unit)
        ids_updated = False

        for nome in clientes:
            nome = clean_text(nome)
            if not nome:
                continue
            best_score  = 0.0
            best_match  = None
            for item in all_contacts:
                item_nome     = item.get("nome", "")
                item_fantasia = item.get("fantasia", "") or ""
                score = max(
                    similarity_score(nome, item_nome),
                    similarity_score(nome, item_fantasia) if item_fantasia else 0.0,
                )
                if score > best_score:
                    best_score = score
                    best_match = item

            if best_score >= threshold and best_match:
                tiny_id = int(best_match["id"])
                ids[nome] = tiny_id
                ids_updated = True
                mapped.append({
                    "clienteNome": nome,
                    "tinyId": tiny_id,
                    "tinyNome": best_match.get("nome", ""),
                    "score": round(best_score, 2),
                })
            else:
                needs_review.append(nome)

        # Salva uma unica vez ao final (evita N escritas em disco)
        if ids_updated:
            _save_extra_cliente_ids(unit, ids)

        return _json({"success": True, "mapped": mapped, "needs_review": needs_review})
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


# ══════════════════════════════════════════════════════════════════════════════
# Snapshots do fechamento (backup/recuperação de planilhas importadas)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/u/<unit>/api/snapshot", methods=["POST"])
@unit_access_required
@csrf_required
def api_snapshot_create(unit: str):
    try:
        data  = request.get_json(force=True, silent=True) or {}
        user  = _current_user() or {}
        now   = dt.datetime.now(ZoneInfo("America/Sao_Paulo"))
        payload = {
            "data":        data.get("data") or now.date().isoformat(),
            "created_at":  now.isoformat(timespec="seconds"),
            "arquivos":    data.get("arquivos") or [],
            "records":     data.get("records") or [],
            "conferencia": data.get("conferencia") or {},
            "conferido":   data.get("conferido") or [],
            "pdv_base":    data.get("pdv_base"),
            "origem":      data.get("origem") or "import",
            "autor":       user.get("email", ""),
        }
        if not payload["records"]:
            raise ValueError("records vazio")
        snap_id = _db_insert_snap(unit, _unit_state_dir(unit), payload)
        return _json({"success": True, "id": snap_id})
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/snapshots", methods=["GET"])
@unit_access_required
def api_snapshot_list(unit: str):
    try:
        date_from = request.args.get("from")
        date_to   = request.args.get("to")
        limit     = int(request.args.get("limit", "200"))
        snaps = _db_list_snap(unit, _unit_state_dir(unit), date_from, date_to, limit)
        return _json({"success": True, "snapshots": snaps})
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/snapshot/<int:snap_id>", methods=["GET"])
@unit_access_required
def api_snapshot_load(unit: str, snap_id: int):
    try:
        snap = _db_load_snap(unit, _unit_state_dir(unit), snap_id)
        if not snap:
            return _json({"success": False, "error": "Snapshot nao encontrado"}, 404)
        return _json({"success": True, "snapshot": snap})
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/snapshot/<int:snap_id>", methods=["DELETE"])
@unit_access_required
@csrf_required
def api_snapshot_delete(unit: str, snap_id: int):
    user = _current_user() or {}
    if not user.get("master"):
        return _json({"success": False, "error": "Apenas usuario master pode remover snapshots"}, 403)
    try:
        ok = _db_delete_snap(unit, _unit_state_dir(unit), snap_id)
        return _json({"success": ok})
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/historico")
@unit_access_required
def historico_page(unit: str):
    return _nocache(send_from_directory(UI_DIR, "historico.html"))


# ══════════════════════════════════════════════════════════════════════════════
# Helpers: caixa do dia (PDV)
# ══════════════════════════════════════════════════════════════════════════════

def _load_caixa_dia(unit: str) -> dict[str, Any]:
    today    = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).date().isoformat()
    unit_dir = _unit_state_dir(unit)

    try:
        n = _db_migrate(unit, unit_dir)
        if n:
            app.logger.info("[caixa] migrated %d records for unit=%s", n, unit)
    except Exception as exc:
        app.logger.error("[caixa] migration FAILED for unit=%s: %s — falling back to JSON", unit, exc)
        p = unit_dir / "caixa_dia.json"
        if p.exists():
            try:
                data = json.loads(p.read_text())
                if data.get("data") == today:
                    return data
            except Exception:
                pass
        return {"data": today, "lancamentos": []}

    _KEEP = {"id", "hora", "timestamp", "placa", "cliente", "cpf", "servico", "valor", "fp", "client_uuid", "usuario"}
    lancamentos = [
        {k: v for k, v in lc.items() if k in _KEEP}
        for lc in _db_load(unit, unit_dir, today)
    ]
    return {"data": today, "lancamentos": lancamentos}


def _append_audit_log(unit: str, acao: str, detalhes: dict[str, Any]) -> None:
    """Registra ação sensível (excluir/editar caixa) em /data/{unit}/audit.log (JSONL)."""
    try:
        unit_dir = _unit_state_dir(unit)
        unit_dir.mkdir(parents=True, exist_ok=True)
        user = _current_user() or {}
        entry = {
            "ts": dt.datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds"),
            "unit": unit,
            "acao": acao,
            "usuario": session.get("email") or user.get("email") or session.get("name") or "",
            "nome": session.get("name") or user.get("name") or "",
            "ip": request.remote_addr or "",
            "detalhes": detalhes,
        }
        with (unit_dir / "audit.log").open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as exc:
        app.logger.error("[audit] falha ao registrar %s unit=%s: %s", acao, unit, exc)


def _save_caixa_dia(unit: str, state: dict[str, Any]) -> None:
    unit_dir = _unit_state_dir(unit)
    today    = state["data"]
    lcs      = state["lancamentos"]
    with _db_connect(unit_dir) as conn:
        conn.execute("DELETE FROM lancamentos WHERE unit=? AND data=?", (unit, today))
        if lcs:
            conn.executemany(
                "INSERT INTO lancamentos "
                "(id,unit,data,hora,timestamp,placa,cliente,cpf,servico,valor,fp,client_uuid,usuario) "
                "VALUES (:id,:unit,:data,:hora,:timestamp,:placa,:cliente,:cpf,:servico,:valor,:fp,:client_uuid,:usuario)",
                [{**lc, "unit": unit, "data": today, "cpf": lc.get("cpf", ""), "client_uuid": lc.get("client_uuid", ""), "usuario": lc.get("usuario", "")} for lc in lcs],
            )


# ── PIN hash seguro ────────────────────────────────────────────────────────────
# Formato novo:  "pbkdf2:<salt_hex>:<dk_hex>"
# Formato legado: qualquer string sem prefixo "pbkdf2:"
# Migração: na primeira verificação correta com legado, o hash é gravado
#           em /data/pins.json e usado em todas as verificações seguintes.
_PINS_FILE = DATA_DIR / "pins.json"
_pins_lock = threading.Lock()

def _load_pin_store() -> dict:
    try:
        return json.loads(_PINS_FILE.read_text()) if _PINS_FILE.exists() else {}
    except Exception:
        return {}

def _save_pin_store(store: dict) -> None:
    try:
        _PINS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _PINS_FILE.write_text(json.dumps(store, indent=2))
    except Exception as e:
        app.logger.warning("Nao foi possivel salvar pins.json: %s", e)

def _hash_pin(pin: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", pin.encode(), salt.encode(), 200_000)
    return f"pbkdf2:{salt}:{dk.hex()}"

def _verify_pin_hash(pin: str, stored: str) -> bool:
    try:
        _, salt, dk_hex = stored.split(":", 2)
        dk = hashlib.pbkdf2_hmac("sha256", pin.encode(), salt.encode(), 200_000)
        return secrets.compare_digest(dk.hex(), dk_hex)
    except Exception:
        return False

def _verify_unit_pin(unit: str, pin: str) -> bool:
    pin = pin.strip()
    if not pin:
        return False

    with _pins_lock:
        store = _load_pin_store()

    # Formato novo — verifica hash
    if unit in store:
        return _verify_pin_hash(pin, store[unit])

    # Formato legado — plaintext do UNITS_CONFIG
    stored_plain = str(UNITS.get(unit, {}).get("master_pin", ""))
    if not stored_plain:
        return False

    if secrets.compare_digest(pin, stored_plain):
        # Migração automática: grava hash e nunca mais usa plaintext
        with _pins_lock:
            store = _load_pin_store()
            store[unit] = _hash_pin(pin)
            _save_pin_store(store)
        return True

    return False


def _caixa_totals(lancamentos: list[dict]) -> dict[str, Any]:
    return calcular_totais(lancamentos)


# ══════════════════════════════════════════════════════════════════════════════
# Fechamento de caixa (trava apos envio Tiny)
# ══════════════════════════════════════════════════════════════════════════════
# Estrutura em /data/<unit>/caixa_fechamento.json:
#   {"2026-04-22": {"fechado_em": "2026-04-22T17:30:00", "fechado_por": "x@y", "motivo": "envio_tiny"}}
# Um dia "fechado" exige PIN master pra lançar, editar, excluir ou reabrir.
# Amanha eh outro dia — _load_caixa_dia carrega nova data automaticamente, sem trava.

def _fechamento_path(unit: str) -> Path:
    return _unit_state_dir(unit) / "caixa_fechamento.json"


def _load_fechamentos(unit: str) -> dict[str, dict]:
    p = _fechamento_path(unit)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_fechamentos(unit: str, data: dict) -> None:
    p = _fechamento_path(unit)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _dia_fechado(unit: str, data_iso: str) -> dict | None:
    """Retorna o dict de fechamento se o dia esta fechado, ou None."""
    return _load_fechamentos(unit).get(data_iso)


def _fechar_dia(unit: str, data_iso: str, user_email: str, motivo: str = "envio_tiny") -> dict:
    """Marca o dia como fechado. Idempotente — nao sobrescreve se ja fechado."""
    fechs = _load_fechamentos(unit)
    if data_iso in fechs:
        return fechs[data_iso]
    entry = {
        "fechado_em":  dt.datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds"),
        "fechado_por": user_email,
        "motivo":      motivo,
    }
    fechs[data_iso] = entry
    _save_fechamentos(unit, fechs)
    return entry


def _reabrir_dia(unit: str, data_iso: str, user_email: str) -> bool:
    """Remove a marcacao de fechado. Retorna True se reabriu, False se nao estava fechado."""
    fechs = _load_fechamentos(unit)
    if data_iso not in fechs:
        return False
    entry = fechs.pop(data_iso)
    # Guarda historico de reaberturas como chave especial (nao afeta lookup do dia)
    hist = fechs.setdefault("_reaberturas", [])
    hist.append({
        **entry,
        "reaberto_em":  dt.datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds"),
        "reaberto_por": user_email,
        "data_original": data_iso,
    })
    _save_fechamentos(unit, fechs)
    return True


# ══════════════════════════════════════════════════════════════════════════════
# Rotas: caixa do dia (PDV)
# ══════════════════════════════════════════════════════════════════════════════

def _nocache(resp):
    resp.headers["Cache-Control"] = "no-store"
    return resp

@app.route("/u/<unit>/caixa")
@unit_access_required
def unit_caixa(unit: str):
    return redirect(f"/u/{unit}/caixa2")


@app.route("/u/<unit>/caixa2")
@unit_access_required
def unit_caixa2(unit: str):
    return _nocache(send_from_directory(UI_DIR, "caixa2.html"))


@app.route("/u/<unit>/manual")
@unit_access_required
def unit_manual(unit: str):
    return send_from_directory(UI_DIR, "manual.html")


@app.route("/manual")
def public_manual():
    return send_from_directory(UI_DIR, "manual.html")


@app.route("/u/<unit>/fechamento")
@unit_access_required
def unit_fechamento(unit: str):
    return _nocache(send_from_directory(UI_DIR, "fechamento.html"))


@app.route("/u/<unit>/api/astro", methods=["POST"])
@unit_access_required
@csrf_required
def api_astro(unit: str):
    """Assistente virtual Astro — powered by Claude Haiku."""
    try:
        import anthropic as _anthropic
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return _json({"success": False, "error": "Assistente nao configurado. Adicione ANTHROPIC_API_KEY nas variaveis do Railway."}, 503)

        data     = request.get_json(force=True, silent=True) or {}
        messages = data.get("messages", [])
        if not messages:
            return _json({"success": False, "error": "messages obrigatorio."}, 400)

        ud        = UNITS.get(unit, {})
        unit_nome = ud.get("nome", unit)
        servicos  = list(ud.get("categoria_ids", {}).keys()) or ud.get("servicos_pdv", [])

        system_prompt = f"""Voce e o Astro, assistente virtual da Astrovistorias — rede de vistorias automotivas.
Voce esta ajudando os atendentes da unidade {unit_nome} a usar o sistema Frente de Caixa.
Responda sempre em portugues brasileiro, de forma direta e simples. Maximo 3 paragrafos curtos.

SISTEMA FRENTE DE CAIXA — VISAO GERAL:
O sistema tem duas telas principais:
1. CAIXA DO DIA (PDV): lancamento de pagamentos em tempo real enquanto o cliente esta na recepcao.
2. FECHAMENTO: importacao da planilha diaria + cruzamento com os lancamentos do PDV + envio para o Tiny ERP.

COMO FAZER UM LANCAMENTO (CAIXA DO DIA):
- Preencha: Placa, Nome do cliente, Servico, Valor
- Clique no botao da forma de pagamento (Dinheiro, Debito, Credito, PIX ou Faturado)
- Clique em "Registrar lancamento"
- O lancamento aparece na tabela abaixo com hora, placa e valor
- Use Tab para navegar entre campos e Enter para avancar

FORMAS DE PAGAMENTO:
- Dinheiro: pagamento em especie
- Debito: cartao de debito
- Credito: cartao de credito
- PIX: transferencia instantanea
- Faturado: sera cobrado depois via nota fiscal (para empresas clientes)
Os lancamentos ficam salvos localmente — NAO vao para o Tiny ainda.

EDITAR OU EXCLUIR LANCAMENTO:
- Clique no icone de lapis (✏️) para editar ou lixeira (🗑️) para excluir
- Sera solicitado o PIN master definido pelo administrador
- Sem PIN configurado, edicao e exclusao ficam bloqueadas

RESUMO DO DIA:
- Clique no botao "Resumo" no topo da tela
- Mostra total por servico, por cliente, por forma de pagamento
- Botao "Copiar" formata o resumo para WhatsApp

FECHAMENTO DO DIA:
- Acesse a tela "Fechamento" pelo botao no topo
- Importe a planilha do dia (arquivo .xls)
- O sistema cruza automaticamente com os lancamentos do PDV
- Divergencias aparecem em vermelho para correcao manual
- Apos correcoes, clique "Enviar para Tiny"

SERVICOS DA UNIDADE {unit_nome.upper()}:
{chr(10).join(f'- {s}' for s in servicos) if servicos else '- Consulte o administrador da unidade'}

DICAS IMPORTANTES:
- Se esquecer o PIN, o administrador pode redefinir nas configuracoes do Railway
- Em caso de erro de conexao com o Tiny, tente novamente em alguns minutos
- Nao feche o navegador no meio de um envio para o Tiny
- Cada lancamento e salvo automaticamente — nao ha botao de "salvar"

Se nao souber responder algo especifico sobre precos ou politicas da empresa, oriente o atendente a perguntar ao administrador/franqueador."""

        client   = _anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=600,
            system=system_prompt,
            messages=messages,
        )
        reply = response.content[0].text if response.content else "Desculpe, nao consegui processar. Tente novamente."
        return _json({"success": True, "reply": reply})

    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/diagnostico-envios-raw")
@unit_access_required
def api_diagnostico_envios_raw(unit: str):
    """Dump bruto do envios_tiny da unidade (zero filtro), pra descobrir por que
    o diagnostico com filtro de data nao retorna nada."""
    try:
        state_dir = _unit_state_dir(unit)
        with _db_connect(state_dir) as conn:
            rows = conn.execute(
                "SELECT unit, data_lancamento, timestamp, placa, cliente, servico, valor, fp, status, erro, chave_deduplicacao, resposta_tiny "
                "FROM envios_tiny WHERE unit=? "
                "ORDER BY timestamp DESC LIMIT 50",
                (unit,),
            ).fetchall()
            total = conn.execute("SELECT COUNT(*) as c FROM envios_tiny WHERE unit=?", (unit,)).fetchone()
            datas = conn.execute(
                "SELECT DISTINCT data_lancamento, COUNT(*) as n FROM envios_tiny WHERE unit=? GROUP BY data_lancamento",
                (unit,),
            ).fetchall()
            statuses = conn.execute(
                "SELECT DISTINCT status, COUNT(*) as n FROM envios_tiny WHERE unit=? GROUP BY status",
                (unit,),
            ).fetchall()
        return _json({
            "unit": unit,
            "total_rows": total["c"] if total else 0,
            "datas_distintas": [dict(r) for r in datas],
            "status_distintos": [dict(r) for r in statuses],
            "ultimos_50": [dict(r) for r in rows],
        })
    except Exception as exc:
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/diagnostico-envios")
@unit_access_required
def api_diagnostico_envios(unit: str):
    """Diagnostico read-only dos envios do dia pra auditoria/conferencia.
    Params: ?data=AAAA-MM-DD (default: hoje)."""
    try:
        data = request.args.get("data") or dt.datetime.now(ZoneInfo("America/Sao_Paulo")).date().isoformat()
        state_dir = _unit_state_dir(unit)
        try:
            envios = _db_load_envios_range(unit, state_dir, data, data)
        except Exception:
            envios = []
        por_status: dict[str, int] = {}
        por_fp:     dict[str, int] = {}
        por_servico: dict[str, int] = {}
        por_cliente: dict[str, list] = {}
        total_valor = 0.0
        for e in envios:
            st = e.get("status", "?")
            fp = e.get("fp", "?")
            sv = (e.get("servico") or "?").upper()
            cl = (e.get("cliente") or "?").upper()
            por_status[st] = por_status.get(st, 0) + 1
            por_fp[fp]     = por_fp.get(fp, 0) + 1
            por_servico[sv] = por_servico.get(sv, 0) + 1
            por_cliente.setdefault(cl, []).append({
                "placa": e.get("placa", ""), "servico": sv,
                "valor": float(e.get("valor", 0) or 0), "fp": fp,
            })
            total_valor += float(e.get("valor", 0) or 0)
        # Flag duplicatas potenciais: mesmo cliente + mesma placa + mesmo servico
        duplicatas = []
        seen: dict[tuple, int] = {}
        for e in envios:
            k = ((e.get("cliente") or "").upper(), (e.get("placa") or "").upper(), (e.get("servico") or "").upper())
            seen[k] = seen.get(k, 0) + 1
        for k, n in seen.items():
            if n > 1:
                duplicatas.append({"cliente": k[0], "placa": k[1], "servico": k[2], "ocorrencias": n})
        return _json({
            "unit": unit, "data": data,
            "total_envios": len(envios),
            "total_valor": round(total_valor, 2),
            "por_status": por_status,
            "por_fp": por_fp,
            "por_servico": por_servico,
            "duplicatas_potenciais": duplicatas,
            "clientes_unicos": len(por_cliente),
            "por_cliente": {cl: items for cl, items in sorted(por_cliente.items())},
        })
    except Exception as exc:
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/caixa/reabrir", methods=["POST"])
@unit_access_required
@csrf_required
def api_caixa_reabrir(unit: str):
    """Reabre o caixa do dia. Exige PIN master."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        ip = request.remote_addr or "unknown"
        if not _pin_rate_check(unit, ip):
            return _json({"success": False, "error": "Muitas tentativas. Aguarde 1 minuto."}, 429)
        if not _verify_unit_pin(unit, data.get("pin", "")):
            return _json({"success": False, "error": "PIN incorreto."}, 403)
        hoje_iso = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).date().isoformat()
        user = _current_user() or {}
        reaberto = _reabrir_dia(unit, hoje_iso, session.get("email", "") or user.get("email", ""))
        if not reaberto:
            return _json({"success": False, "error": "Caixa nao estava fechado."}, 400)
        return _json({"success": True, "message": "Caixa reaberto."})
    except Exception as exc:
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/caixa/estado")
@unit_access_required
def api_caixa_estado(unit: str):
    try:
        state = _load_caixa_dia(unit)
        fech = _dia_fechado(unit, state["data"])
        return _json({
            "success": True,
            "data": state["data"],
            "lancamentos": state["lancamentos"],
            "totais": _caixa_totals(state["lancamentos"]),
            "fechado": bool(fech),
            "fechamento": fech,
        })
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/caixa/lancar", methods=["POST"])
@unit_access_required
@csrf_required
def api_caixa_lancar(unit: str):
    try:
        data    = request.get_json(force=True, silent=True) or {}
        placa   = clean_text(data.get("placa", "")).upper()
        cliente = clean_text(data.get("cliente", "")).upper()
        cpf     = "".join(c for c in data.get("cpf", "") if c.isdigit())[:14]
        servico = clean_text(data.get("servico", "")).upper()
        valor   = float(data.get("valor", 0))
        fp      = data.get("fp", "")
        client_uuid = clean_text(data.get("client_uuid", ""))[:64]

        err = validar_lancamento({"placa": placa, "cliente": cliente, "servico": servico,
                                   "valor": valor, "fp": fp})
        if err:
            return _json({"success": False, "error": err}, 400)

        # Se o dia esta fechado (envio Tiny ja aconteceu), exige PIN pra lançar
        hoje_iso = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).date().isoformat()
        fech_info = _dia_fechado(unit, hoje_iso)
        if fech_info:
            ip = request.remote_addr or "unknown"
            if not _pin_rate_check(unit, ip):
                return _json({"success": False, "error": "Muitas tentativas. Aguarde 1 minuto."}, 429)
            if not _verify_unit_pin(unit, data.get("pin", "")):
                return _json({
                    "success": False,
                    "error": "Caixa fechado — PIN master necessário para novo lançamento.",
                    "reason": "caixa_fechado",
                    "fechamento": fech_info,
                }, 403)

        state = _load_caixa_dia(unit)

        # Dedup idempotente: se o client ja enviou esse client_uuid, retorna o existente.
        # Protege contra retry depois de timeout/rede falha — evita duplicar no dia.
        if client_uuid:
            for lc in state["lancamentos"]:
                if lc.get("client_uuid") == client_uuid:
                    return _json({
                        "success": True,
                        "lancamento": lc,
                        "totais": _caixa_totals(state["lancamentos"]),
                        "total_lancamentos": len(state["lancamentos"]),
                        "deduped": True,
                    })

        now = dt.datetime.now(ZoneInfo("America/Sao_Paulo"))
        user = _current_user() or {}
        usuario_tag = (session.get("email") or user.get("email") or session.get("name") or "")[:120]
        lancamento = {
            "id": secrets.token_hex(8),
            "hora": now.strftime("%H:%M"),
            "timestamp": now.isoformat(),
            "placa": placa,
            "cliente": cliente,
            "cpf": cpf,
            "servico": servico,
            "valor": round(valor, 2),
            "fp": fp,
            "usuario": usuario_tag,
        }
        if client_uuid:
            lancamento["client_uuid"] = client_uuid
        state["lancamentos"].append(lancamento)
        _save_caixa_dia(unit, state)

        return _json({
            "success": True,
            "lancamento": lancamento,
            "totais": _caixa_totals(state["lancamentos"]),
            "total_lancamentos": len(state["lancamentos"]),
        })
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/caixa/editar/<lancamento_id>", methods=["PUT"])
@unit_access_required
@csrf_required
def api_caixa_editar(unit: str, lancamento_id: str):
    try:
        data = request.get_json(force=True, silent=True) or {}
        ip = request.remote_addr or "unknown"
        if not _pin_rate_check(unit, ip):
            return _json({"success": False, "error": "Muitas tentativas. Aguarde 1 minuto."}, 429)
        if not _verify_unit_pin(unit, data.get("pin", "")):
            return _json({"success": False, "error": "PIN incorreto."}, 403)

        placa   = clean_text(data.get("placa", "")).upper()
        cliente = clean_text(data.get("cliente", "")).upper()
        cpf     = "".join(c for c in data.get("cpf", "") if c.isdigit())[:14]
        servico = clean_text(data.get("servico", "")).upper()
        valor   = float(data.get("valor", 0))
        fp      = data.get("fp", "")

        if not all([placa, cliente, servico]) or valor <= 0 or fp not in FP_VALIDOS:
            return _json({"success": False, "error": "Dados invalidos."}, 400)

        state = _load_caixa_dia(unit)
        for lc in state["lancamentos"]:
            if lc["id"] == lancamento_id:
                antes = {"placa": lc.get("placa", ""), "cliente": lc.get("cliente", ""),
                         "servico": lc.get("servico", ""), "valor": lc.get("valor", 0), "fp": lc.get("fp", "")}
                lc.update({"placa": placa, "cliente": cliente, "cpf": cpf,
                            "servico": servico, "valor": round(valor, 2), "fp": fp})
                _save_caixa_dia(unit, state)
                _append_audit_log(unit, "editar_lancamento", {
                    "lancamento_id": lancamento_id,
                    "antes": antes,
                    "depois": {"placa": placa, "cliente": cliente, "servico": servico,
                               "valor": round(valor, 2), "fp": fp},
                    "lancado_por": lc.get("usuario", ""),
                })
                return _json({"success": True, "totais": _caixa_totals(state["lancamentos"])})

        return _json({"success": False, "error": "Lancamento nao encontrado."}, 404)
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/caixa/excluir/<lancamento_id>", methods=["DELETE"])
@unit_access_required
@csrf_required
def api_caixa_excluir(unit: str, lancamento_id: str):
    try:
        data = request.get_json(force=True, silent=True) or {}
        ip = request.remote_addr or "unknown"
        if not _pin_rate_check(unit, ip):
            return _json({"success": False, "error": "Muitas tentativas. Aguarde 1 minuto."}, 429)
        if not _verify_unit_pin(unit, data.get("pin", "")):
            return _json({"success": False, "error": "PIN incorreto."}, 403)

        state = _load_caixa_dia(unit)
        antes = len(state["lancamentos"])
        removido = next((lc for lc in state["lancamentos"] if lc["id"] == lancamento_id), None)
        state["lancamentos"] = [lc for lc in state["lancamentos"] if lc["id"] != lancamento_id]
        if len(state["lancamentos"]) == antes:
            return _json({"success": False, "error": "Lancamento nao encontrado."}, 404)

        _save_caixa_dia(unit, state)
        _append_audit_log(unit, "excluir_lancamento", {
            "lancamento_id": lancamento_id,
            "placa": (removido or {}).get("placa", ""),
            "valor": (removido or {}).get("valor", 0),
            "servico": (removido or {}).get("servico", ""),
            "fp": (removido or {}).get("fp", ""),
            "lancado_por": (removido or {}).get("usuario", ""),
        })
        return _json({
            "success": True,
            "totais": _caixa_totals(state["lancamentos"]),
            "total_lancamentos": len(state["lancamentos"]),
        })
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/caixa/conferir", methods=["POST"])
@unit_access_required
@csrf_required
def api_caixa_conferir(unit: str):
    """Cruza registros AV da planilha com lancamentos do PDV do dia.

    Input:  { records: [{id, placa, servico, preco, fp}] }
    Output: { conferencia: { <record_id>: { status, pdv_valor, pdv_fp, pdv_hora } } }

    status:
      "ok"                — placa+servico encontrado no PDV, valor igual
      "ok_fallback"       — match por placa+valor (servico divergia; planilha manda)
      "divergencia_valor" — encontrado no PDV mas valor difere
      "divergencia_fp"    — FP do PDV difere da planilha (AV vs FA)
      "sem_pdv"           — nem (placa,servico) nem (placa,valor) acharam par no PDV
    """
    try:
        import re
        import unicodedata as _ud

        data    = request.get_json(force=True, silent=True) or {}
        records = data.get("records", [])
        config  = _build_unit_config(unit)

        # Data alvo: prioriza data da planilha (se vier no body) ou a data dos
        # proprios records; fallback eh hoje. Assim fechamentos retroativos
        # (importar planilha de ontem hoje) conseguem cruzar com o PDV do dia
        # correto.
        target_date = (data.get("data") or "").strip()
        if not target_date and records:
            for r in records:
                d = (r.get("data") or "").strip()
                if d:
                    target_date = d[:10]
                    break
        unit_dir = _unit_state_dir(unit)
        if target_date:
            try:
                lancamentos = _db_load(unit, unit_dir, target_date)
            except Exception:
                lancamentos = []
        else:
            caixa = _load_caixa_dia(unit)
            lancamentos = caixa.get("lancamentos", [])

        def _norm_placa(value: str) -> str:
            return re.sub(r"[^A-Z0-9]", "", clean_text(value).upper())

        def _norm_servico(value: str) -> str:
            v = clean_text(value).upper()
            v = apply_alias(config, "servico", v)
            v = _ud.normalize("NFD", v)
            v = "".join(c for c in v if _ud.category(c) != "Mn")
            return " ".join(v.split())

        # Indice PDV: (placa_norm, servico_norm) → lancamento
        # Se houver duplicatas no PDV (nao deveria, mas por seguranca), mantemos o ultimo
        pdv_map: dict[tuple, dict] = {}
        for lc in lancamentos:
            key = (_norm_placa(lc.get("placa", "")), _norm_servico(lc.get("servico", "")))
            pdv_map[key] = lc

        def _find_pdv_key(placa: str, servico: str) -> tuple | None:
            """Busca exata primeiro; fallback por prefixo para tolerar truncamento do Excel."""
            exact = (placa, servico)
            if exact in pdv_map:
                return exact
            # Truncamento do Excel limita nomes a ~19 chars — compara pelo comprimento menor
            for (p, s) in pdv_map:
                if p != placa:
                    continue
                n = min(len(s), len(servico))
                if n >= 12 and s[:n] == servico[:n]:
                    return (p, s)
            return None

        def _planilha_has_match(placa: str, servico: str, planilha_keys: set) -> bool:
            if (placa, servico) in planilha_keys:
                return True
            for (p, s) in planilha_keys:
                if p != placa:
                    continue
                n = min(len(s), len(servico))
                if n >= 12 and s[:n] == servico[:n]:
                    return True
            return False

        # Primeiro pass: match por (placa, servico). Guarda pdv_keys consumidas
        # para segundo pass por placa+valor (fallback quando servico diverge).
        consumed_pdv_keys: set[tuple] = set()
        planilha_keys: set[tuple] = set()
        conferencia: dict[str, dict] = {}
        # Pendentes para fallback por placa+valor: (rec_id, placa, preco, planilha_fp)
        pending_fallback: list[tuple] = []
        for r in records:
            planilha_fp = r.get("fp", "AV")   # "AV" ou "FA"
            rec_id  = r.get("id", "")
            placa   = _norm_placa(r.get("placa", ""))
            servico = _norm_servico(r.get("servico", ""))
            preco   = float(r.get("preco", 0))
            planilha_keys.add((placa, servico))

            pdv_key = _find_pdv_key(placa, servico)
            if pdv_key is None:
                # Adia decisao: tenta fallback por placa+valor apos o loop
                pending_fallback.append((rec_id, placa, preco, planilha_fp))
                conferencia[rec_id] = {
                    "status": "sem_pdv",
                    "pdv_valor": None,
                    "pdv_fp": None,
                    "pdv_hora": None,
                }
            else:
                consumed_pdv_keys.add(pdv_key)
                lc        = pdv_map[pdv_key]
                pdv_valor = float(lc.get("valor", 0))
                pdv_fp    = lc.get("fp", "")
                # Categoriza FP do PDV em AV ou FA (faturado → FA, resto → AV)
                pdv_fp_cat = "FA" if pdv_fp in ("faturado", "detran") else "AV"

                if pdv_fp_cat != planilha_fp:
                    status = "divergencia_fp"
                elif abs(pdv_valor - preco) >= 0.01:
                    status = "divergencia_valor"
                else:
                    status = "ok"

                conferencia[rec_id] = {
                    "status": status,
                    "pdv_valor": pdv_valor,
                    "pdv_fp": pdv_fp,
                    "pdv_hora": lc.get("hora"),
                }

        # Segundo pass (fallback): casa planilha "sem_pdv" com PDV restante
        # por (placa, valor aprox). Resolve caso em que o servico diverge
        # entre planilha e PDV (ex: Ian lancou "LAUDO DE TRANSFERENCIA" no PDV
        # e a planilha tem "LAUDO DE TRANSFERENCIA COM VISTORIA").
        for rec_id, placa, preco, planilha_fp in pending_fallback:
            for pdv_key, lc in pdv_map.items():
                if pdv_key in consumed_pdv_keys:
                    continue
                if pdv_key[0] != placa:
                    continue
                pdv_valor = float(lc.get("valor", 0))
                if abs(pdv_valor - preco) >= 0.01:
                    continue
                # Match por placa+valor
                consumed_pdv_keys.add(pdv_key)
                pdv_fp = lc.get("fp", "")
                pdv_fp_cat = "FA" if pdv_fp in ("faturado", "detran") else "AV"
                status = "ok_fallback" if pdv_fp_cat == planilha_fp else "divergencia_fp"
                conferencia[rec_id] = {
                    "status": status,
                    "pdv_valor": pdv_valor,
                    "pdv_fp": pdv_fp,
                    "pdv_hora": lc.get("hora"),
                    "pdv_servico_original": lc.get("servico"),  # dica para debug
                }
                break

        # Lançamentos do PDV sem nenhum correspondente na planilha (AV ou FA)
        # — so entram aqui PDVs que NAO foram consumidos no match principal
        # nem no fallback por placa+valor. Ou seja: servicos avulsos genuinos
        # (PESQUISA AVULSA, BAIXA PERMANENTE) e pagamentos sem planilha.
        pdv_sem_planilha = []
        for pdv_key, lc in pdv_map.items():
            if pdv_key in consumed_pdv_keys:
                continue
            pdv_sem_planilha.append({
                "pdv_id":   lc.get("id"),
                "hora":     lc.get("hora"),
                "placa":    lc.get("placa"),
                "cliente":  lc.get("cliente"),
                "servico":  lc.get("servico"),
                "valor":    lc.get("valor"),
                "fp":       lc.get("fp"),
                "timestamp": lc.get("timestamp"),
            })

        return _json({"success": True, "conferencia": conferencia, "pdv_sem_planilha": pdv_sem_planilha})
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


# ══════════════════════════════════════════════════════════════════════════════
# Rotas: gerencial (acesso restrito — master da unidade ou global)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/u/<unit>/gerencial")
@gerencial_required
def unit_gerencial(unit: str):
    return _nocache(send_from_directory(UI_DIR, "gerencial.html"))


@app.route("/u/<unit>/api/gerencial/historico")
@gerencial_required
def api_gerencial_historico(unit: str):
    """Histórico financeiro: envios_tiny até ontem (verdade consolidada) + PDV de hoje (parcial).

    Dias anteriores: fonte é `envios_tiny` (o que foi efetivamente ao Tiny ERP).
    Hoje: fonte é o PDV (`lancamentos`), porque ainda não foi enviado ao Tiny.
    """
    try:
        date_from = request.args.get("from", "")
        date_to   = request.args.get("to", "")
        dt.date.fromisoformat(date_from)
        dt.date.fromisoformat(date_to)

        unit_dir = _unit_state_dir(unit)
        today    = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).date().isoformat()

        # Corte: envios_tiny de date_from até min(date_to, ontem). PDV só do dia de hoje.
        # Se date_to < hoje: usa todo o range em envios_tiny, sem PDV.
        # Se date_to >= hoje: envios até ontem + PDV de hoje.
        incluir_hoje = (date_to >= today >= date_from)
        envios_ate = min(date_to, (dt.date.fromisoformat(today) - dt.timedelta(days=1)).isoformat()) if incluir_hoje else date_to

        def _norm_fp(fp: str) -> str:
            f = (fp or "").strip().lower()
            if f == "fa": return "faturado"
            if f == "av": return "avista"
            return f

        registros: list[dict] = []
        if envios_ate >= date_from:
            envios = _db_load_envios_range(unit, unit_dir, date_from, envios_ate)
            for e in envios:
                registros.append({
                    "data":    e.get("data_lancamento") or "",
                    "placa":   e.get("placa", ""),
                    "cliente": e.get("cliente", ""),
                    "servico": e.get("servico", ""),
                    "valor":   float(e.get("valor", 0) or 0),
                    "fp":      _norm_fp(e.get("fp", "")),
                    "fonte":   "tiny",
                })
        if incluir_hoje:
            for lc in _db_load_range(unit, unit_dir, today, today):
                registros.append({**lc, "fonte": "pdv"})

        fp_keys = ("dinheiro", "debito", "credito", "pix", "faturado", "detran", "avista")
        totais: dict[str, float] = {fp: 0.0 for fp in fp_keys}
        for lc in registros:
            fp = lc.get("fp", "")
            if fp in totais:
                totais[fp] += float(lc.get("valor", 0))

        total  = sum(totais.values())
        avista = total - totais["faturado"] - totais["detran"]
        count  = len(registros)

        by_day: dict[str, list] = {}
        for lc in registros:
            by_day.setdefault(lc.get("data", ""), []).append(lc)

        por_dia = []
        for data in sorted(by_day.keys()):
            dlcs = by_day[data]
            dt_fp: dict[str, float] = {fp: 0.0 for fp in fp_keys}
            for lc in dlcs:
                fp = lc.get("fp", "")
                if fp in dt_fp:
                    dt_fp[fp] += float(lc.get("valor", 0))
            dtotal = sum(dt_fp.values())
            fontes = {lc.get("fonte", "") for lc in dlcs}
            por_dia.append({
                "data":      data,
                "total":     round(dtotal, 2),
                "avista":    round(dtotal - dt_fp["faturado"] - dt_fp["detran"], 2),
                "faturado":  round(dt_fp["faturado"], 2),
                "detran":    round(dt_fp["detran"], 2),
                "dinheiro":  round(dt_fp["dinheiro"], 2),
                "debito":    round(dt_fp["debito"], 2),
                "credito":   round(dt_fp["credito"], 2),
                "pix":       round(dt_fp["pix"], 2),
                "count":     len(dlcs),
                "fonte":     "pdv" if fontes == {"pdv"} else ("tiny" if fontes == {"tiny"} else "misto"),
                "parcial":   data == today and "pdv" in fontes,
                "lancamentos": dlcs,
            })

        svc_count: dict[str, int]   = {}
        svc_total: dict[str, float] = {}
        for lc in registros:
            s = (lc.get("servico", "") or "").strip()
            if s:
                svc_count[s] = svc_count.get(s, 0) + 1
                svc_total[s] = svc_total.get(s, 0.0) + float(lc.get("valor", 0))
        servicos = sorted(
            [{"servico": s, "count": svc_count[s], "total": round(svc_total[s], 2)}
             for s in svc_count],
            key=lambda x: x["count"], reverse=True,
        )

        ud = UNITS.get(unit, {})
        return _json({
            "success":  True,
            "unidade":  ud.get("nome", unit),
            "periodo":  {"from": date_from, "to": date_to},
            "fonte":    {"tiny_ate": envios_ate, "pdv_hoje": incluir_hoje, "hoje": today},
            "resumo": {
                "total":        round(total, 2),
                "avista":       round(avista, 2),
                "faturado":     round(totais["faturado"], 2),
                "detran":       round(totais["detran"], 2),
                "dinheiro":     round(totais["dinheiro"], 2),
                "debito":       round(totais["debito"], 2),
                "credito":      round(totais["credito"], 2),
                "pix":          round(totais["pix"], 2),
                "count":        count,
                "ticket_medio": round(total / count, 2) if count else 0.0,
            },
            "por_dia":  por_dia,
            "servicos": servicos,
        })
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/gerencial/exportar")
@gerencial_required
def api_gerencial_exportar(unit: str):
    try:
        import csv, io
        date_from = request.args.get("from", "")
        date_to   = request.args.get("to", "")
        dt.date.fromisoformat(date_from)
        dt.date.fromisoformat(date_to)

        unit_dir = _unit_state_dir(unit)
        today    = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).date().isoformat()
        incluir_hoje = (date_to >= today >= date_from)
        envios_ate = min(date_to, (dt.date.fromisoformat(today) - dt.timedelta(days=1)).isoformat()) if incluir_hoje else date_to

        def _norm_fp(fp):
            f = (fp or "").strip().lower()
            if f == "fa": return "faturado"
            if f == "av": return "avista"
            return f

        linhas = []
        if envios_ate >= date_from:
            for e in _db_load_envios_range(unit, unit_dir, date_from, envios_ate):
                linhas.append({
                    "data": e.get("data_lancamento", ""), "hora": "",
                    "placa": e.get("placa", ""), "cliente": e.get("cliente", ""),
                    "cpf": "", "servico": e.get("servico", ""),
                    "valor": e.get("valor", 0), "fp": _norm_fp(e.get("fp", "")),
                    "fonte": "tiny",
                })
        if incluir_hoje:
            for lc in _db_load_range(unit, unit_dir, today, today):
                linhas.append({**lc, "fonte": "pdv"})

        out = io.StringIO()
        w   = csv.writer(out)
        w.writerow(["Data", "Hora", "Placa", "Cliente", "CPF/CNPJ", "Servico", "Valor", "FP", "Fonte"])
        for lc in linhas:
            w.writerow([
                lc.get("data", ""), lc.get("hora", ""), lc.get("placa", ""),
                lc.get("cliente", ""), lc.get("cpf", ""), lc.get("servico", ""),
                lc.get("valor", ""), lc.get("fp", ""), lc.get("fonte", ""),
            ])

        nome = UNITS.get(unit, {}).get("nome", unit)
        fname = f"historico_{nome}_{date_from}_{date_to}.csv"
        return Response(
            "\ufeff" + out.getvalue(),
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{fname}"'},
        )
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


# ══════════════════════════════════════════════════════════════════════════════
# Rotas: divergências de fechamento
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/u/<unit>/api/divergencias/registrar", methods=["POST"])
@unit_access_required
@csrf_required
def api_divergencias_registrar(unit: str):
    try:
        data = request.get_json(force=True, silent=True) or {}
        now  = dt.datetime.now(ZoneInfo("America/Sao_Paulo"))
        div  = {
            "id":        secrets.token_hex(8),
            "unit":      unit,
            "data":      now.date().isoformat(),
            "timestamp": now.isoformat(),
            "placa":     clean_text(data.get("placa", "")).upper(),
            "cliente":   clean_text(data.get("cliente", "")).upper(),
            "servico":   clean_text(data.get("servico", "")).upper(),
            "valor":     float(data.get("valor", 0)),
            "fp":        data.get("fp", ""),
            "motivo":    data.get("motivo", ""),
            "pdv_valor": data.get("pdv_valor"),
            "pdv_fp":    data.get("pdv_fp", ""),
            "arquivo":   data.get("arquivo", ""),
        }
        _db_insert_div(_unit_state_dir(unit), div)
        return _json({"success": True})
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/gerencial/divergencias")
@gerencial_required
def api_gerencial_divergencias(unit: str):
    try:
        date_from = request.args.get("from", "")
        date_to   = request.args.get("to", "")
        dt.date.fromisoformat(date_from)
        dt.date.fromisoformat(date_to)
        divs = _db_load_div(unit, _unit_state_dir(unit), date_from, date_to)
        return _json({"success": True, "divergencias": divs})
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


# ══════════════════════════════════════════════════════════════════════════════
# Rotas: gerencial master (visão consolidada da rede — master: true)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/gerencial")
@master_view_required
def master_gerencial_page():
    return _nocache(send_from_directory(UI_DIR, "master_gerencial.html"))


@app.route("/gerencial/historico-caixa")
@master_view_required
def master_historico_caixa_page():
    return _nocache(send_from_directory(UI_DIR, "historico-caixa.html"))


@app.route("/gerencial/bi")
@master_view_required
def master_bi_page():
    return _nocache(send_from_directory(UI_DIR, "bi.html"))


@app.route("/gerencial/historico-emitido")
@master_view_required
def master_historico_emitido_page():
    return _nocache(send_from_directory(UI_DIR, "historico-emitido.html"))


@app.route("/master/usuarios-conectados")
@master_view_required
def master_usuarios_conectados_page():
    return _nocache(send_from_directory(UI_DIR, "usuarios_conectados.html"))


@app.route("/master/api/usuarios-conectados")
@master_view_required
def master_api_usuarios_conectados():
    ativos = _get_active_users()
    now_ts = time.time()
    out = []
    for info in ativos:
        idle = int(now_ts - info["last_seen"])
        out.append({
            "email":     info["email"],
            "nome":      info["nome"],
            "unit":      info["unit"],
            "master":    info["master"],
            "gerencial": info["gerencial"],
            "idle_seconds": idle,
            "last_path": info["last_path"],
            "last_ip":   info["last_ip"],
        })
    return _json({
        "usuarios": out,
        "total":    len(out),
        "ttl_seconds": _ACTIVE_TTL_SECONDS,
    })


@app.route("/master/api/usuarios-conectados.csv")
@master_view_required
def master_api_usuarios_conectados_csv():
    """Export CSV dos usuarios ativos neste instante (janela de 3 min)."""
    import csv, io
    ativos = _get_active_users()
    now_ts = time.time()
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL)
    w.writerow(["email", "nome", "perfil", "unidade", "idle_segundos", "ultima_rota", "ultimo_ip"])
    for info in ativos:
        if info.get("master"):        perfil = "master"
        elif info.get("matriz"):      perfil = "matriz"
        elif info.get("gerencial"):   perfil = "gerencial"
        else:                         perfil = "operador"
        w.writerow([
            info.get("email", ""),
            info.get("nome", ""),
            perfil,
            info.get("unit", ""),
            int(now_ts - info.get("last_seen", now_ts)),
            info.get("last_path", ""),
            info.get("last_ip", ""),
        ])
    hoje = dt.date.today().isoformat()
    resp = Response("﻿" + buf.getvalue(), mimetype="text/csv; charset=utf-8")
    resp.headers["Content-Disposition"] = f'attachment; filename="conectados_astro_{hoje}.csv"'
    return resp


def _sessoes_agg(periodo_raw: str) -> tuple[list[dict], str]:
    """Agrega sessoes por usuario (JSONL + em andamento). Retorna (lista_ordenada, periodo_efetivo)."""
    periodo = (periodo_raw or "7d").lower()
    tz = ZoneInfo("America/Sao_Paulo")
    now_dt = dt.datetime.now(tz)
    if periodo == "hoje":
        cutoff = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif periodo == "30d":
        cutoff = now_dt - dt.timedelta(days=30)
    elif periodo in ("tudo", "all"):
        cutoff = None
    else:
        periodo = "7d"
        cutoff = now_dt - dt.timedelta(days=7)

    agg: dict[str, dict[str, Any]] = {}

    def _bucket(email: str, template: dict[str, Any]) -> dict[str, Any]:
        return agg.setdefault(email, {
            "email":     email,
            "nome":      template.get("nome") or email,
            "unit":      template.get("unit") or "",
            "master":    bool(template.get("master")),
            "gerencial": bool(template.get("gerencial")),
            "logins":    0,
            "total_s":   0,
            "last":      "",
        })

    # 1) Le o JSONL (sessoes encerradas)
    if _SESSION_LOG_PATH.exists():
        try:
            with _SESSION_LOG_LOCK:
                with _SESSION_LOG_PATH.open("r", encoding="utf-8") as f:
                    lines = f.readlines()
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except Exception:
                    continue
                if cutoff is not None:
                    try:
                        started = dt.datetime.fromisoformat(entry.get("started_at", ""))
                        if started < cutoff:
                            continue
                    except Exception:
                        continue
                email = (entry.get("email") or "").lower()
                if not email:
                    continue
                a = _bucket(email, entry)
                a["logins"]  += 1
                a["total_s"] += int(entry.get("duration_s") or 0)
                ended = entry.get("ended_at") or ""
                if ended > a["last"]:
                    a["last"] = ended
                # Mantem identidade mais recente se disponivel
                if entry.get("nome"):
                    a["nome"] = entry["nome"]
                if entry.get("unit"):
                    a["unit"] = entry["unit"]
        except Exception as exc:
            app.logger.error("[api sessoes] falha ao ler JSONL: %s", exc)

    # 2) Soma sessoes em andamento
    with _ACTIVE_USERS_LOCK:
        now_ts = time.time()
        for email, info in _ACTIVE_USERS.items():
            started_ts = info.get("session_start") or info.get("last_seen")
            if cutoff is not None and started_ts:
                started_dt = dt.datetime.fromtimestamp(started_ts, tz)
                if started_dt < cutoff:
                    continue
            duration = max(0, int(now_ts - (started_ts or now_ts)))
            a = _bucket(email, info)
            a["logins"]  += 1
            a["total_s"] += duration
            ended = dt.datetime.fromtimestamp(now_ts, tz).isoformat(timespec="seconds")
            if ended > a["last"]:
                a["last"] = ended

    # 3) Ordena e calcula media
    out = []
    for a in agg.values():
        avg = int(a["total_s"] / a["logins"]) if a["logins"] else 0
        out.append({**a, "avg_s": avg})
    out.sort(key=lambda x: (-x["logins"], -x["total_s"], x["email"]))
    return out, periodo


@app.route("/master/api/usuarios-sessoes")
@master_view_required
def master_api_usuarios_sessoes():
    """Agregacoes por usuario das sessoes ja encerradas (JSONL) + sessoes em
    andamento (_ACTIVE_USERS). Suporta filtro de periodo: hoje | 7d | 30d | tudo."""
    out, periodo = _sessoes_agg(request.args.get("periodo", ""))
    return _json({
        "usuarios": out,
        "periodo":  periodo,
        "total":    len(out),
    })


@app.route("/master/api/usuarios-sessoes.csv")
@master_view_required
def master_api_usuarios_sessoes_csv():
    """Export CSV do historico de sessoes por usuario. Mesmo filtro de periodo."""
    import csv, io
    out, periodo = _sessoes_agg(request.args.get("periodo", ""))
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL)
    w.writerow(["email", "nome", "unidade", "perfil", "logins", "tempo_total_s", "tempo_total_hms", "media_s", "ultima_sessao"])
    for u in out:
        if u.get("master"):          perfil = "master"
        elif u.get("matriz"):        perfil = "matriz"
        elif u.get("gerencial"):     perfil = "gerencial"
        else:                        perfil = "operador"
        total = int(u.get("total_s") or 0)
        h, rem = divmod(total, 3600)
        m, s   = divmod(rem, 60)
        hms = f"{h:02d}:{m:02d}:{s:02d}"
        w.writerow([
            u.get("email", ""),
            u.get("nome", ""),
            u.get("unit", ""),
            perfil,
            u.get("logins", 0),
            total,
            hms,
            u.get("avg_s", 0),
            u.get("last", ""),
        ])
    hoje = dt.date.today().isoformat()
    resp = Response("﻿" + buf.getvalue(), mimetype="text/csv; charset=utf-8")
    resp.headers["Content-Disposition"] = f'attachment; filename="sessoes_astro_{periodo}_{hoje}.csv"'
    return resp


# ── CRUD de usuarios ───────────────────────────────────────────────────────────
def _user_public(email: str, u: dict[str, Any]) -> dict[str, Any]:
    """Serializa usuario sem hash de senha."""
    return {
        "email":     email,
        "name":      u.get("name", email),
        "unit":      u.get("unit", ""),
        "master":    bool(u.get("master")),
        "matriz":    bool(u.get("matriz")),
        "gerencial": bool(u.get("gerencial")),
    }


def _validate_user_payload(data: dict[str, Any], editing: bool) -> tuple[str, dict[str, Any]] | tuple[None, str]:
    """Valida payload. Retorna (email_normalizado, dados_saneados) ou (None, erro_msg)."""
    email = (data.get("email") or "").strip().lower()
    name  = (data.get("name")  or "").strip()
    unit  = (data.get("unit")  or "").strip()
    master    = bool(data.get("master"))
    matriz    = bool(data.get("matriz"))
    gerencial = bool(data.get("gerencial"))
    password  = (data.get("password") or "")

    if not email or "@" not in email:
        return None, "E-mail inválido."
    if not name:
        return None, "Nome é obrigatório."
    if not master and not matriz and not unit:
        return None, "Usuário não-master precisa de unidade."
    if unit and unit not in UNITS:
        return None, f"Unidade '{unit}' não existe."
    if not editing and len(password) < 8:
        return None, "Senha deve ter pelo menos 8 caracteres."
    if editing and password and len(password) < 8:
        return None, "Nova senha deve ter pelo menos 8 caracteres."

    sanit = {
        "name":      name,
        "unit":      "" if (master or matriz) else unit,
        "master":    master,
        "matriz":    matriz and not master,
        "gerencial": gerencial or master or matriz,
    }
    if password:
        sanit["password_hash"] = _hash_password(password)
    return email, sanit


@app.route("/master/usuarios")
@master_view_required
def master_usuarios_page():
    return _nocache(send_from_directory(UI_DIR, "usuarios.html"))


def _usuarios_perfil_str(u: dict[str, Any]) -> str:
    if u.get("master"):    return "master"
    if u.get("matriz"):    return "matriz"
    if u.get("gerencial"): return "gerencial"
    return "operador"


def _usuarios_filtered() -> list[dict]:
    """Aplica os mesmos filtros opcionais do JSON (perfil, unit, q)."""
    f_perfil = (request.args.get("perfil") or "").strip().lower()
    f_unit   = (request.args.get("unit")   or "").strip()
    f_q      = (request.args.get("q")      or "").strip().lower()
    with _USERS_LOCK:
        out = []
        for email, u in sorted(USERS.items()):
            if u.get("convidado") or not u.get("password_hash"):
                continue
            if f_perfil and _usuarios_perfil_str(u) != f_perfil:
                continue
            if f_unit and (u.get("unit") or "") != f_unit:
                continue
            if f_q and f_q not in email.lower() and f_q not in (u.get("name", "").lower()):
                continue
            out.append((email, u))
    return out


@app.route("/master/api/usuarios", methods=["GET"])
@master_view_required
def master_api_usuarios_list():
    unidades = [{"id": uid, "nome": UNITS[uid].get("nome", uid)} for uid in sorted(UNITS.keys())]
    filtered = _usuarios_filtered()
    users = [_user_public(e, u) for e, u in filtered]
    return _json({"usuarios": users, "unidades": unidades})


@app.route("/master/api/usuarios.csv")
@master_view_required
def master_api_usuarios_csv():
    """Export CSV com os mesmos filtros opcionais (perfil, unit, q) da rota JSON."""
    import csv, io
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL)
    w.writerow(["email", "nome", "perfil", "unidade_id", "unidade_nome"])
    for email, u in _usuarios_filtered():
        unit_id = u.get("unit") or ""
        unit_nome = UNITS.get(unit_id, {}).get("nome", unit_id) if unit_id else ""
        w.writerow([email, u.get("name", ""), _usuarios_perfil_str(u), unit_id, unit_nome])
    hoje = dt.date.today().isoformat()
    resp = Response("﻿" + buf.getvalue(), mimetype="text/csv; charset=utf-8")
    resp.headers["Content-Disposition"] = f'attachment; filename="usuarios_astro_{hoje}.csv"'
    return resp


def _is_privileged_target(sanit_new: dict[str, Any], current: dict[str, Any] | None = None) -> bool:
    """True se o alvo (estado resultante ou atual) tem flag master ou matriz.
    Matriz editando um master/matriz, ou promovendo alguem a master/matriz, precisa aprovacao."""
    if sanit_new.get("master") or sanit_new.get("matriz"):
        return True
    if current and (current.get("master") or current.get("matriz")):
        return True
    return False


@app.route("/master/api/usuarios", methods=["POST"])
@matriz_or_master
@csrf_required
def master_api_usuarios_create():
    me = _current_user() or {}
    me_email = session.get("email", "")
    me["email"] = me_email  # _write_audit_log espera email no user
    data = request.get_json(silent=True) or {}
    result = _validate_user_payload(data, editing=False)
    if result[0] is None:
        return _json({"success": False, "error": result[1]}, 400)
    email, sanit = result

    # Matriz criando usuario privilegiado -> pending approval
    if not me.get("master") and _is_privileged_target(sanit):
        with _USERS_LOCK:
            if email in USERS:
                return _json({"success": False, "error": "Já existe usuário com este e-mail."}, 409)
        approval_id = _create_pending_approval(
            me, "user.create", email,
            {"sanit": sanit},
            description=f"Criar usuário {email} com perfil privilegiado (master/matriz)",
        )
        return _json({
            "success":     True,
            "pending":     True,
            "approval_id": approval_id,
            "message":     "Seu perfil matriz nao pode criar usuario master/matriz diretamente. O pedido foi enviado para aprovacao do master.",
        })

    with _USERS_LOCK:
        if email in USERS:
            return _json({"success": False, "error": "Já existe usuário com este e-mail."}, 409)
        USERS[email] = sanit
        _save_users(USERS)
    app.logger.info(f"[usuarios] criado: {email}")
    _write_audit_log(me, "user.create", email, {k: v for k, v in sanit.items() if k != "password_hash"})
    return _json({"success": True, "usuario": _user_public(email, sanit)})


@app.route("/master/api/usuarios/<path:email>", methods=["PUT"])
@matriz_or_master
@csrf_required
def master_api_usuarios_update(email: str):
    me = _current_user() or {}
    me_email = session.get("email", "")
    me["email"] = me_email
    email = (email or "").strip().lower()
    data = request.get_json(silent=True) or {}
    data["email"] = email
    result = _validate_user_payload(data, editing=True)
    if result[0] is None:
        return _json({"success": False, "error": result[1]}, 400)
    _, sanit = result
    with _USERS_LOCK:
        if email not in USERS:
            return _json({"success": False, "error": "Usuário não encontrado."}, 404)
        current = dict(USERS[email])

    # Matriz editando user privilegiado (atual ou resultante) -> pending
    if not me.get("master") and _is_privileged_target(sanit, current):
        approval_id = _create_pending_approval(
            me, "user.update", email,
            {"sanit": sanit},
            description=f"Editar usuário {email} com perfil privilegiado",
        )
        return _json({
            "success":     True,
            "pending":     True,
            "approval_id": approval_id,
            "message":     "Seu perfil matriz nao pode editar um usuario master/matriz diretamente. O pedido foi enviado para aprovacao do master.",
        })

    with _USERS_LOCK:
        # Proteger ultimo master: se estava master e vai deixar de ser, confirmar que tem outro
        if USERS[email].get("master") and not sanit.get("master"):
            outros_masters = [e for e, u in USERS.items() if e != email and u.get("master")]
            if not outros_masters:
                return _json({"success": False, "error": "Não é possível remover o status master do último master."}, 400)
        # Se nao tem nova senha no payload, preserva hash atual
        if "password_hash" not in sanit:
            sanit["password_hash"] = USERS[email].get("password_hash", "")
        USERS[email] = sanit
        _save_users(USERS)
    app.logger.info(f"[usuarios] atualizado: {email}")
    _write_audit_log(me, "user.update", email, {k: v for k, v in sanit.items() if k != "password_hash"})
    return _json({"success": True, "usuario": _user_public(email, sanit)})


@app.route("/master/api/usuarios/<path:email>", methods=["DELETE"])
@master_only_required
@csrf_required
def master_api_usuarios_delete(email: str):
    email = (email or "").strip().lower()
    me_email = session.get("email", "").lower()
    if email == me_email:
        return _json({"success": False, "error": "Você não pode excluir a própria conta."}, 400)
    with _USERS_LOCK:
        if email not in USERS:
            return _json({"success": False, "error": "Usuário não encontrado."}, 404)
        if USERS[email].get("master"):
            outros_masters = [e for e, u in USERS.items() if e != email and u.get("master")]
            if not outros_masters:
                return _json({"success": False, "error": "Não é possível excluir o último master."}, 400)
        USERS.pop(email, None)
        _save_users(USERS)
    app.logger.info(f"[usuarios] removido: {email}")
    me = {**(_current_user() or {}), "email": me_email}
    _write_audit_log(me, "user.delete", email, {})
    return _json({"success": True})


# ═══ Auditoria e aprovacoes ══════════════════════════════════════════════════
@app.route("/master/auditoria")
@master_view_required
def master_auditoria_page():
    return _nocache(send_from_directory(UI_DIR, "auditoria.html"))


def _auditoria_filtered() -> list[dict]:
    """Aplica filtros (email, action, from, to) e permissao de visibilidade.
    Usado pelo JSON e pelo export CSV."""
    user = _current_user() or {}
    me_email = session.get("email", "").lower()
    entries = _read_audit_log(limit=5000)
    if not user.get("master"):
        entries = [e for e in entries if (e.get("user_email") or "").lower() == me_email]
    filtro_email = (request.args.get("email") or "").strip().lower()
    filtro_acao  = (request.args.get("action") or "").strip()
    filtro_from  = (request.args.get("from") or "").strip()  # YYYY-MM-DD
    filtro_to    = (request.args.get("to")   or "").strip()
    if filtro_email:
        entries = [e for e in entries if (e.get("user_email") or "").lower() == filtro_email]
    if filtro_acao:
        entries = [e for e in entries if (e.get("action") or "").startswith(filtro_acao)]
    if filtro_from:
        entries = [e for e in entries if (e.get("ts") or "") >= filtro_from]
    if filtro_to:
        # inclui o dia inteiro do 'to'
        entries = [e for e in entries if (e.get("ts") or "") <= filtro_to + "T23:59:59"]
    return entries


@app.route("/master/api/auditoria")
@master_view_required
def master_api_auditoria():
    """Retorna as ultimas N entradas do audit log, mais recentes primeiro.
    Matriz ve apenas as proprias acoes; master ve tudo."""
    entries = _auditoria_filtered()
    return _json({"entries": entries[:500], "total": len(entries)})


@app.route("/master/api/auditoria.csv")
@master_view_required
def master_api_auditoria_csv():
    """Export CSV com as mesmas regras de filtro/permissao da rota JSON."""
    import csv, io
    entries = _auditoria_filtered()
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL)
    w.writerow(["quando", "email", "nome", "perfil", "acao", "alvo", "resultado", "approval_id", "ip", "payload_json"])
    for e in entries:
        payload = e.get("payload") or {}
        payload_str = json.dumps(payload, ensure_ascii=False) if payload else ""
        w.writerow([
            e.get("ts", ""),
            e.get("user_email", ""),
            e.get("user_name", ""),
            e.get("user_role", ""),
            e.get("action", ""),
            e.get("target", ""),
            e.get("result", ""),
            e.get("approval_id", ""),
            e.get("ip", ""),
            payload_str,
        ])
    csv_text = "﻿" + buf.getvalue()  # BOM para Excel abrir UTF-8 direito
    hoje = dt.date.today().isoformat()
    resp = Response(csv_text, mimetype="text/csv; charset=utf-8")
    resp.headers["Content-Disposition"] = f'attachment; filename="auditoria_astro_{hoje}.csv"'
    return resp


@app.route("/master/aprovacoes")
@master_only_required
def master_aprovacoes_page():
    return _nocache(send_from_directory(UI_DIR, "aprovacoes.html"))


@app.route("/master/api/aprovacoes", methods=["GET"])
@master_only_required
def master_api_aprovacoes_list():
    entries = _approvals_read_all()
    # Mais recentes primeiro
    entries.sort(key=lambda e: e.get("created_at", ""), reverse=True)
    pend = [e for e in entries if e.get("status") == "pending"]
    hist = [e for e in entries if e.get("status") != "pending"][:200]
    return _json({"pending": pend, "historico": hist, "total_pending": len(pend)})


@app.route("/master/api/aprovacoes/<approval_id>", methods=["POST"])
@master_only_required
@csrf_required
def master_api_aprovacoes_decidir(approval_id: str):
    """Aprova ou rejeita um pedido pendente. body: {decisao: 'aprovar'|'rejeitar', reason?: str}"""
    body = request.get_json(silent=True) or {}
    decisao = (body.get("decisao") or "").strip().lower()
    reason  = (body.get("reason")  or "").strip()
    if decisao not in ("aprovar", "rejeitar"):
        return _json({"success": False, "error": "Decisao invalida (use aprovar ou rejeitar)."}, 400)

    entries = _approvals_read_all()
    target = next((e for e in entries if e.get("id") == approval_id), None)
    if not target:
        return _json({"success": False, "error": "Aprovacao nao encontrada."}, 404)
    if target.get("status") != "pending":
        return _json({"success": False, "error": f"Aprovacao ja foi {target.get('status')}."}, 409)

    me_email = session.get("email", "").lower()
    me = {**(_current_user() or {}), "email": me_email}
    tz = ZoneInfo("America/Sao_Paulo")
    target["status"]      = "approved" if decisao == "aprovar" else "rejected"
    target["reviewed_by"] = me_email
    target["reviewed_at"] = dt.datetime.now(tz).isoformat(timespec="seconds")
    target["reason"]      = reason

    # Se aprovado, executar a acao
    exec_result = "ok"
    exec_error  = ""
    if decisao == "aprovar":
        try:
            _execute_approved_action(target)
        except Exception as exc:
            exec_result = "error"
            exec_error  = str(exc)
            target["reason"] = (reason + " | ERRO: " + str(exc)).strip(" |")
            app.logger.error("[approvals] erro ao executar acao aprovada %s: %s", approval_id, exc)

    # Reescreve arquivo com a entry atualizada
    out = []
    for e in entries:
        out.append(target if e.get("id") == approval_id else e)
    _approvals_write_all(out)

    _write_audit_log(
        me, f"approval.{target['status']}:{target.get('action','')}",
        target.get("target", ""),
        {"requested_by": target.get("requested_by", ""), "reason": reason},
        result=exec_result,
        approval_id=approval_id,
    )
    return _json({
        "success":     exec_result == "ok",
        "status":      target["status"],
        "exec_result": exec_result,
        "exec_error":  exec_error,
    })


def _execute_approved_action(entry: dict[str, Any]) -> None:
    """Executa a acao descrita em uma pending approval aprovada. Levanta excecao em caso de erro."""
    action  = entry.get("action", "")
    target  = entry.get("target", "")
    payload = entry.get("payload", {}) or {}
    # Usuario que originou a acao — para registrar no audit como 'executada em nome de'
    origem = {
        "email": entry.get("requested_by", ""),
        "name":  entry.get("requested_name", ""),
        "matriz": True,
    }
    if action == "user.create":
        sanit = payload.get("sanit", {})
        with _USERS_LOCK:
            if target in USERS:
                raise RuntimeError(f"Usuario {target} ja existe — criacao nao pode seguir.")
            USERS[target] = sanit
            _save_users(USERS)
        _write_audit_log(origem, "user.create", target,
                         {k: v for k, v in sanit.items() if k != "password_hash"},
                         approval_id=entry.get("id", ""))
    elif action == "user.update":
        sanit = payload.get("sanit", {})
        with _USERS_LOCK:
            if target not in USERS:
                raise RuntimeError(f"Usuario {target} nao existe.")
            if USERS[target].get("master") and not sanit.get("master"):
                outros = [e for e, u in USERS.items() if e != target and u.get("master")]
                if not outros:
                    raise RuntimeError("Nao e possivel remover status master do ultimo master.")
            if "password_hash" not in sanit:
                sanit["password_hash"] = USERS[target].get("password_hash", "")
            USERS[target] = sanit
            _save_users(USERS)
        _write_audit_log(origem, "user.update", target,
                         {k: v for k, v in sanit.items() if k != "password_hash"},
                         approval_id=entry.get("id", ""))
    elif action == "user.invite":
        sanit = payload.get("sanit", {})
        with _USERS_LOCK:
            if target in USERS and USERS[target].get("password_hash"):
                raise RuntimeError(f"Usuario {target} ja esta ativo.")
            stub = {**sanit, "password_hash": "", "convidado": True}
            USERS[target] = stub
            _save_users(USERS)
        token, invite = _gerar_convite(target, sanit, origem.get("email", ""))
        try:
            _send_invite_email(target, sanit.get("name", ""), token, origem.get("name", ""))
        except Exception as exc:
            app.logger.error("[convite] falha ao enviar email aprovado %s: %s", target, exc)
        _write_audit_log(origem, "user.invite", target,
                         {k: v for k, v in sanit.items() if k != "password_hash"},
                         approval_id=entry.get("id", ""))
    else:
        raise RuntimeError(f"Acao desconhecida: {action}")


# ── Convites por email ─────────────────────────────────────────────────────────

def _gerar_convite(email: str, sanit: dict, criador_email: str) -> tuple[str, dict]:
    """Cria convite novo (revoga pendentes anteriores pro mesmo email). Retorna (token, invite_dict)."""
    now = dt.datetime.now(ZoneInfo("America/Sao_Paulo"))
    token = secrets.token_urlsafe(32)
    invite = {
        "email": email,
        "name": sanit["name"],
        "perfil": {k: sanit[k] for k in ("master", "matriz", "gerencial", "unit")},
        "criado_por": criador_email,
        "criado_em": now.isoformat(timespec="seconds"),
        "expira_em": (now + dt.timedelta(hours=_INVITE_TTL_HOURS)).isoformat(timespec="seconds"),
        "usado_em": None,
        "revogado_em": None,
    }
    with _INVITES_LOCK:
        invites = _load_invites()
        # Revoga convites antigos pendentes do mesmo email
        for t, inv in invites.items():
            if inv.get("email") == email and not inv.get("usado_em") and not inv.get("revogado_em"):
                inv["revogado_em"] = now.isoformat(timespec="seconds")
        invites[token] = invite
        _save_invites(invites)
    return token, invite


@app.route("/master/api/usuarios/convite", methods=["POST"])
@matriz_or_master
@csrf_required
def master_api_usuarios_convite():
    """Cria convite: valida payload (como usuário), gera token, envia email.
    Usuário fica com status 'convidado' (sem password_hash) até ativar.

    Matriz pode convidar perfil gerencial/unidade direto; convite pra master ou
    matriz precisa aprovacao."""
    data = request.get_json(silent=True) or {}
    data["email"] = (data.get("email") or "").strip().lower()
    data["_skip_pwd"] = True
    result = _validate_user_payload_convite(data)
    if result[0] is None:
        return _json({"success": False, "error": result[1]}, 400)
    email, sanit = result

    me = _current_user() or {}
    me_email = session.get("email", "").lower()
    me["email"] = me_email
    criador_nome  = session.get("name", "")

    # Matriz convidando pra alvo privilegiado — passa por aprovacao
    if not me.get("master") and _is_privileged_target(sanit):
        approval_id = _create_pending_approval(
            {**me, "name": criador_nome},
            "user.invite", email,
            {"sanit": sanit},
            description=f"Convidar {email} com perfil privilegiado (master/matriz)",
        )
        return _json({
            "success":       True,
            "approval_id":   approval_id,
            "pending":       True,
            "message":       "Seu perfil matriz nao pode convidar usuario master/matriz diretamente. O pedido foi enviado para aprovacao do master.",
        })

    with _USERS_LOCK:
        if email in USERS and USERS[email].get("password_hash"):
            return _json({"success": False, "error": "Já existe usuário ativo com este e-mail."}, 409)
        stub = {**sanit, "password_hash": "", "convidado": True}
        USERS[email] = stub
        _save_users(USERS)
    token, invite = _gerar_convite(email, sanit, me_email)
    email_enviado = True
    erro_email = ""
    link = ""
    try:
        _send_invite_email(email, sanit["name"], token, criador_nome)
    except Exception as exc:
        email_enviado = False
        erro_email = str(exc)
        link = f"{_public_base_url()}/ativar/{token}"
        app.logger.error("[convite] falha ao enviar email %s: %s", email, exc)

    _write_audit_log(me, "user.invite", email,
                     {k: v for k, v in sanit.items() if k != "password_hash"},
                     result="ok" if email_enviado else "error")
    app.logger.info(f"[convite] enviado: {email} (por {me_email})")
    payload = {"success": True, "email_enviado": email_enviado, "expira_em": invite["expira_em"]}
    if not email_enviado:
        payload["erro_email"] = erro_email
        payload["link"]       = link
    return _json(payload)


def _validate_user_payload_convite(data: dict) -> tuple[str, dict] | tuple[None, str]:
    """Validação especial pra convite: não exige senha."""
    email = (data.get("email") or "").strip().lower()
    name  = (data.get("name")  or "").strip()
    unit  = (data.get("unit")  or "").strip()
    master    = bool(data.get("master"))
    matriz    = bool(data.get("matriz"))
    gerencial = bool(data.get("gerencial"))
    if not email or "@" not in email:
        return None, "E-mail inválido."
    if not name:
        return None, "Nome é obrigatório."
    if not master and not matriz and not unit:
        return None, "Usuário não-master precisa de unidade."
    if unit and unit not in UNITS:
        return None, f"Unidade '{unit}' não existe."
    sanit = {
        "name":      name,
        "unit":      "" if (master or matriz) else unit,
        "master":    master,
        "matriz":    matriz and not master,
        "gerencial": gerencial or master or matriz,
    }
    return email, sanit


@app.route("/master/api/convites", methods=["GET"])
@master_only_required
def master_api_convites_list():
    """Lista convites (pendentes primeiro, depois histórico)."""
    out = []
    with _INVITES_LOCK:
        invites = _load_invites()
    for token, inv in invites.items():
        out.append({
            "token_short": token[:10],
            "token": token,
            "email": inv.get("email", ""),
            "name": inv.get("name", ""),
            "perfil": inv.get("perfil", {}),
            "criado_por": inv.get("criado_por", ""),
            "criado_em": inv.get("criado_em", ""),
            "expira_em": inv.get("expira_em", ""),
            "status": _invite_status(inv),
        })
    out.sort(key=lambda i: (i["status"] != "pendente", i["criado_em"]), reverse=False)
    out.sort(key=lambda i: i["criado_em"], reverse=True)
    return _json({"convites": out})


@app.route("/master/api/convites.csv")
@master_only_required
def master_api_convites_csv():
    """Export CSV de convites. Filtro opcional ?status=pendente|usado|expirado|revogado."""
    import csv, io
    f_status = (request.args.get("status") or "").strip().lower()
    with _INVITES_LOCK:
        invites = _load_invites()
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL)
    w.writerow(["email", "nome", "perfil", "unidade", "criado_por", "criado_em", "expira_em", "status", "token_short"])
    rows = []
    for token, inv in invites.items():
        status = _invite_status(inv)
        if f_status and status != f_status:
            continue
        perfil = inv.get("perfil", {}) or {}
        if perfil.get("master"):       perfil_str = "master"
        elif perfil.get("matriz"):     perfil_str = "matriz"
        elif perfil.get("gerencial"):  perfil_str = "gerencial"
        else:                          perfil_str = "operador"
        rows.append({
            "email":      inv.get("email", ""),
            "nome":       inv.get("name", ""),
            "perfil":     perfil_str,
            "unidade":    perfil.get("unit", ""),
            "criado_por": inv.get("criado_por", ""),
            "criado_em":  inv.get("criado_em", ""),
            "expira_em":  inv.get("expira_em", ""),
            "status":     status,
            "token_short": token[:10],
        })
    rows.sort(key=lambda r: r["criado_em"], reverse=True)
    for r in rows:
        w.writerow([r["email"], r["nome"], r["perfil"], r["unidade"], r["criado_por"],
                    r["criado_em"], r["expira_em"], r["status"], r["token_short"]])
    hoje = dt.date.today().isoformat()
    resp = Response("﻿" + buf.getvalue(), mimetype="text/csv; charset=utf-8")
    resp.headers["Content-Disposition"] = f'attachment; filename="convites_astro_{hoje}.csv"'
    return resp


@app.route("/master/api/convites/<token>", methods=["DELETE"])
@master_only_required
@csrf_required
def master_api_convites_revoke(token: str):
    now = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds")
    with _INVITES_LOCK:
        invites = _load_invites()
        inv = invites.get(token)
        if not inv:
            return _json({"success": False, "error": "Convite não encontrado."}, 404)
        if inv.get("usado_em"):
            return _json({"success": False, "error": "Convite já foi usado."}, 400)
        inv["revogado_em"] = now
        _save_invites(invites)
        # Remove o stub de usuário se ainda não ativou
        email = inv.get("email", "")
        with _USERS_LOCK:
            u = USERS.get(email)
            if u and not u.get("password_hash"):
                USERS.pop(email, None)
                _save_users(USERS)
    app.logger.info(f"[convite] revogado: {inv.get('email')}")
    return _json({"success": True})


@app.route("/master/api/convites/<token>/reenviar", methods=["POST"])
@master_only_required
@csrf_required
def master_api_convites_reenviar(token: str):
    """Gera novo token pro mesmo email/perfil e envia novo email."""
    with _INVITES_LOCK:
        invites = _load_invites()
        inv = invites.get(token)
        if not inv:
            return _json({"success": False, "error": "Convite não encontrado."}, 404)
        if inv.get("usado_em"):
            return _json({"success": False, "error": "Convite já foi usado."}, 400)
    # Regenera
    sanit_stub = {
        "name": inv.get("name", ""),
        **inv.get("perfil", {}),
    }
    criador_email = session.get("email", "")
    criador_nome  = session.get("name", "")
    new_token, new_invite = _gerar_convite(inv["email"], sanit_stub, criador_email)
    try:
        _send_invite_email(inv["email"], sanit_stub["name"], new_token, criador_nome)
    except Exception as exc:
        link = f"{_public_base_url()}/ativar/{new_token}"
        return _json({
            "success": True,
            "email_enviado": False,
            "erro_email": str(exc),
            "link": link,
            "expira_em": new_invite["expira_em"],
        })
    return _json({"success": True, "email_enviado": True, "expira_em": new_invite["expira_em"]})


# ── Rotas públicas de ativação (não exigem login) ──────────────────────────────

@app.route("/ativar/<token>")
def ativar_page(token: str):
    return _nocache(send_from_directory(UI_DIR, "ativar.html"))


@app.route("/api/ativar/<token>", methods=["GET"])
def api_ativar_info(token: str):
    with _INVITES_LOCK:
        invites = _load_invites()
        inv = invites.get(token)
    if not inv:
        return _json({"success": False, "error": "Convite não encontrado.", "motivo": "invalido"}, 404)
    if inv.get("usado_em"):
        return _json({"success": False, "error": "Este convite já foi usado.", "motivo": "usado"}, 410)
    if inv.get("revogado_em"):
        return _json({"success": False, "error": "Este convite foi revogado.", "motivo": "revogado"}, 410)
    if not _invite_is_valid(inv):
        return _json({"success": False, "error": "Este convite expirou. Peça ao administrador para gerar um novo.", "motivo": "expirado"}, 410)
    return _json({
        "success": True,
        "email": inv.get("email", ""),
        "name": inv.get("name", ""),
        "expira_em": inv.get("expira_em", ""),
    })


@app.route("/api/ativar/<token>", methods=["POST"])
def api_ativar_confirm(token: str):
    data = request.get_json(silent=True) or {}
    senha = str(data.get("password") or "")
    if len(senha) < 8:
        return _json({"success": False, "error": "Senha deve ter pelo menos 8 caracteres."}, 400)
    with _INVITES_LOCK:
        invites = _load_invites()
        inv = invites.get(token)
        if not inv:
            return _json({"success": False, "error": "Convite não encontrado."}, 404)
        if inv.get("usado_em") or inv.get("revogado_em") or not _invite_is_valid(inv):
            return _json({"success": False, "error": "Convite inválido ou expirado."}, 410)
        email = inv["email"]
        now_iso = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds")
        # Monta sanit final
        perfil = inv.get("perfil", {})
        sanit = {
            "name":      inv.get("name", ""),
            "unit":      perfil.get("unit", ""),
            "master":    bool(perfil.get("master")),
            "matriz":    bool(perfil.get("matriz")),
            "gerencial": bool(perfil.get("gerencial")),
            "password_hash": _hash_password(senha),
        }
        with _USERS_LOCK:
            USERS[email] = sanit
            _save_users(USERS)
        inv["usado_em"] = now_iso
        _save_invites(invites)
    # Auto-login
    session.clear()
    session.permanent = True
    session["email"] = email
    session["name"]  = sanit["name"]
    app.logger.info(f"[convite] ativado: {email}")
    return _json({"success": True, "email": email})


def _agg_lancamentos(lancamentos: list[dict], fp_keys: tuple) -> dict:
    totais: dict[str, float] = {fp: 0.0 for fp in fp_keys}
    for lc in lancamentos:
        fp = lc.get("fp", "")
        if fp in totais:
            totais[fp] += float(lc.get("valor", 0))
    total  = sum(totais.values())
    avista = total - totais["faturado"] - totais["detran"]
    count  = len(lancamentos)
    return {**totais, "total": round(total, 2), "avista": round(avista, 2),
            "count": count, "ticket_medio": round(total / count, 2) if count else 0.0}


def _norm_fp_tiny(fp: str) -> str:
    """Normaliza fp vindo de envios_tiny: 'fa'->'faturado', 'av'->'avista'."""
    f = (fp or "").strip().lower()
    if f == "fa": return "faturado"
    if f == "av": return "avista"
    return f


def _load_unit_range_dual(uid: str, unit_dir, date_from: str, date_to: str, today: str) -> tuple[list[dict], str, bool]:
    """Carrega lançamentos de uma unidade cruzando envios_tiny (dias passados) + PDV (hoje).

    Espelha a regra do gerencial por unidade: dias < hoje vêm de envios_tiny (verdade
    consolidada pós-envio); hoje vem do PDV (ao vivo, ainda não enviado).
    Retorna (registros, envios_ate, incluir_hoje).
    """
    incluir_hoje = (date_to >= today >= date_from)
    envios_ate   = min(date_to, (dt.date.fromisoformat(today) - dt.timedelta(days=1)).isoformat()) if incluir_hoje else date_to

    registros: list[dict] = []
    if envios_ate >= date_from:
        try:
            for e in _db_load_envios_range(uid, unit_dir, date_from, envios_ate):
                registros.append({
                    "data":      e.get("data_lancamento") or "",
                    "placa":     e.get("placa", ""),
                    "cliente":   e.get("cliente", ""),
                    "servico":   e.get("servico", ""),
                    "valor":     float(e.get("valor", 0) or 0),
                    "fp":        _norm_fp_tiny(e.get("fp", "")),
                    "timestamp": e.get("timestamp", ""),
                    "fonte":     "tiny",
                })
        except Exception:
            app.logger.warning("[gerencial-master] falha lendo envios_tiny unit=%s", uid)
    if incluir_hoje:
        try:
            for lc in _db_load_range(uid, unit_dir, today, today):
                registros.append({**lc, "fonte": "pdv"})
        except Exception:
            app.logger.warning("[gerencial-master] falha lendo PDV unit=%s", uid)
    return registros, envios_ate, incluir_hoje


@app.route("/gerencial/api/historico")
@master_view_required
def api_master_historico():
    try:
        date_from   = request.args.get("from", "")
        date_to     = request.args.get("to", "")
        unit_filter = request.args.get("unit", "all")
        detail      = request.args.get("detail", "").strip() in ("1", "true", "yes")
        dt.date.fromisoformat(date_from)
        dt.date.fromisoformat(date_to)

        fp_keys = ("dinheiro", "debito", "credito", "pix", "faturado", "detran", "avista")
        today   = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).date().isoformat()

        units_to_query = list(UNITS.keys()) if unit_filter == "all" else (
            [unit_filter] if unit_filter in UNITS else []
        )

        all_lcs: list[dict] = []
        por_unidade: list[dict] = []
        envios_ate_global = date_to
        incluir_hoje_global = False
        for uid in units_to_query:
            ud = UNITS[uid]
            lcs, envios_ate, incluir_hoje = _load_unit_range_dual(
                uid, _unit_state_dir(uid), date_from, date_to, today,
            )
            envios_ate_global = envios_ate
            incluir_hoje_global = incluir_hoje_global or incluir_hoje
            for lc in lcs:
                lc["unit_slug"] = uid
                lc["unit_nome"] = ud.get("nome", uid)
            all_lcs.extend(lcs)
            agg = _agg_lancamentos(lcs, fp_keys)
            por_unidade.append({"unit": uid, "nome": ud.get("nome", uid), **agg})

        por_unidade.sort(key=lambda x: x["total"], reverse=True)

        resumo = _agg_lancamentos(all_lcs, fp_keys)

        # Por dia
        by_day: dict[str, list] = {}
        for lc in all_lcs:
            by_day.setdefault(lc.get("data", ""), []).append(lc)

        por_dia = []
        for data in sorted(by_day.keys()):
            dlcs = by_day[data]
            dagg = _agg_lancamentos(dlcs, fp_keys)
            dt_fp: dict[str, float] = {fp: 0.0 for fp in fp_keys}
            for lc in dlcs:
                fp = lc.get("fp", "")
                if fp in dt_fp:
                    dt_fp[fp] += float(lc.get("valor", 0))
            fontes = {lc.get("fonte", "") for lc in dlcs}
            por_dia.append({"data": data, **dagg,
                            **{fp: round(dt_fp[fp], 2) for fp in fp_keys},
                            "fonte":   "pdv" if fontes == {"pdv"} else ("tiny" if fontes == {"tiny"} else "misto"),
                            "parcial": data == today and "pdv" in fontes,
                            "lancamentos": dlcs})

        # Ranking serviços
        svc_count: dict[str, int]   = {}
        svc_total: dict[str, float] = {}
        for lc in all_lcs:
            s = (lc.get("servico", "") or "").strip()
            if s:
                svc_count[s] = svc_count.get(s, 0) + 1
                svc_total[s] = svc_total.get(s, 0.0) + float(lc.get("valor", 0))
        servicos = sorted(
            [{"servico": s, "count": svc_count[s], "total": round(svc_total[s], 2)}
             for s in svc_count],
            key=lambda x: x["count"], reverse=True,
        )

        payload = {
            "success":      True,
            "unidades":     {uid: UNITS[uid].get("nome", uid) for uid in UNITS},
            "unit_filter":  unit_filter,
            "periodo":      {"from": date_from, "to": date_to},
            "fonte":        {"tiny_ate": envios_ate_global, "pdv_hoje": incluir_hoje_global, "hoje": today},
            "resumo":       resumo,
            "por_unidade":  por_unidade,
            "por_dia":      por_dia,
            "servicos":     servicos,
        }

        if detail:
            all_sorted = sorted(
                all_lcs,
                key=lambda x: (x.get("data", ""), x.get("hora", ""), x.get("timestamp", "")),
            )
            payload["lancamentos"] = all_sorted

        return _json(payload)
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/gerencial/api/exportar")
@master_view_required
def api_master_exportar():
    try:
        import csv, io
        date_from   = request.args.get("from", "")
        date_to     = request.args.get("to", "")
        unit_filter = request.args.get("unit", "all")
        dt.date.fromisoformat(date_from)
        dt.date.fromisoformat(date_to)

        today = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).date().isoformat()
        units_to_query = list(UNITS.keys()) if unit_filter == "all" else (
            [unit_filter] if unit_filter in UNITS else []
        )
        all_lcs: list[dict] = []
        for uid in units_to_query:
            lcs, _envios_ate, _incluir_hoje = _load_unit_range_dual(
                uid, _unit_state_dir(uid), date_from, date_to, today,
            )
            for lc in lcs:
                lc["unit_nome"] = UNITS[uid].get("nome", uid)
            all_lcs.extend(lcs)
        all_lcs.sort(key=lambda x: (x.get("data", ""), x.get("timestamp", "")))

        out = io.StringIO()
        w   = csv.writer(out)
        w.writerow(["Unidade", "Data", "Hora", "Placa", "Cliente", "CPF/CNPJ", "Servico", "Valor", "FP", "Fonte"])
        for lc in all_lcs:
            w.writerow([
                lc.get("unit_nome", ""), lc.get("data", ""), lc.get("hora", ""),
                lc.get("placa", ""), lc.get("cliente", ""), lc.get("cpf", ""),
                lc.get("servico", ""), lc.get("valor", ""), lc.get("fp", ""),
                lc.get("fonte", ""),
            ])

        label = unit_filter if unit_filter != "all" else "rede"
        fname = f"historico_{label}_{date_from}_{date_to}.csv"
        return Response(
            "\ufeff" + out.getvalue(),
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{fname}"'},
        )
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/gerencial/api/divergencias")
@master_view_required
def api_master_divergencias():
    try:
        date_from   = request.args.get("from", "")
        date_to     = request.args.get("to", "")
        unit_filter = request.args.get("unit", "all")
        dt.date.fromisoformat(date_from)
        dt.date.fromisoformat(date_to)
        units_to_query = list(UNITS.keys()) if unit_filter == "all" else (
            [unit_filter] if unit_filter in UNITS else []
        )
        all_divs = []
        for uid in units_to_query:
            try:
                divs = _db_load_div(uid, _unit_state_dir(uid), date_from, date_to)
                for d in divs:
                    d["unit_nome"] = UNITS[uid].get("nome", uid)
                all_divs.extend(divs)
            except Exception:
                pass
        all_divs.sort(key=lambda x: x.get("timestamp", ""))
        return _json({"success": True, "divergencias": all_divs})
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


# ══════════════════════════════════════════════════════════════════════════════
# Alerta de fechamento — cron interno 18:30 SP
# ══════════════════════════════════════════════════════════════════════════════

def _email_provider() -> str:
    """Retorna 'resend' se RESEND_API_KEY setada, senao 'smtp' se SMTP_* setadas, senao ''."""
    if os.environ.get("RESEND_API_KEY", "").strip():
        return "resend"
    if all(os.environ.get(k, "").strip() for k in ("SMTP_HOST", "SMTP_USER", "SMTP_PASS")):
        return "smtp"
    return ""


def _send_via_resend(subject: str, html: str, recipients: list[str],
                     attachment: bytes | None = None, attachment_name: str = "") -> None:
    """Envia via API HTTPS do Resend. Porta 443, funciona em qualquer cloud."""
    import base64, json as _json_mod, urllib.request, urllib.error
    api_key    = os.environ.get("RESEND_API_KEY", "").strip()
    from_email = os.environ.get("RESEND_FROM", "").strip() or "Astrovistorias <onboarding@resend.dev>"
    payload = {
        "from": from_email,
        "to": recipients,
        "subject": subject,
        "html": html,
    }
    if attachment and attachment_name:
        payload["attachments"] = [{
            "filename": attachment_name,
            "content": base64.b64encode(attachment).decode("ascii"),
        }]
    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=_json_mod.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
            "User-Agent":    "AstroVistorias/1.0 (+https://astrovistorias.com.br)",
            "Accept":        "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            app.logger.info("[email:resend] ok subject=%s to=%s resp=%s", subject, recipients, body[:200])
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        app.logger.error("[email:resend] HTTP %s subject=%s: %s", exc.code, subject, body[:500])
        raise RuntimeError(f"Resend HTTP {exc.code}: {body[:300]}") from exc


def _send_via_smtp(subject: str, html: str, recipients: list[str],
                   attachment: bytes | None = None, attachment_name: str = "") -> None:
    """Envia via SMTP_SSL. Requer SMTP_HOST/PORT/USER/PASS."""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText as _MIMEText
    from email.mime.base import MIMEBase
    from email import encoders
    host   = os.environ.get("SMTP_HOST", "")
    port   = int(os.environ.get("SMTP_PORT", "465") or 465)
    user   = os.environ.get("SMTP_USER", "")
    passwd = os.environ.get("SMTP_PASS", "")
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"]    = f"Astrovistorias <{user}>"
    msg["To"]      = ", ".join(recipients)
    msg.attach(_MIMEText(html, "html", "utf-8"))
    if attachment and attachment_name:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment)
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{attachment_name}"')
        msg.attach(part)
    with smtplib.SMTP_SSL(host, port) as smtp:
        smtp.login(user, passwd)
        smtp.sendmail(user, recipients, msg.as_string())
    app.logger.info("[email:smtp] ok subject=%s to=%s", subject, recipients)


def _public_base_url() -> str:
    """URL publica do sistema (usada em links de email).
    Prefere PUBLIC_BASE_URL env; senao deriva de request.host_url (trailing slash removida).
    """
    env_url = os.environ.get("PUBLIC_BASE_URL", "").strip().rstrip("/")
    if env_url:
        return env_url
    try:
        return (request.host_url or "").rstrip("/")
    except RuntimeError:
        return "https://astro-v2.up.railway.app"


def _send_invite_email(email: str, nome: str, token: str, criador_nome: str) -> None:
    """Envia convite com link /ativar/<token>. Levanta RuntimeError se provider ausente."""
    base = _public_base_url()
    link = f"{base}/ativar/{token}"
    html = f"""
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;max-width:560px;margin:0 auto;padding:20px;color:#111827">
      <h2 style="color:#0e7490;margin:0 0 8px">Bem-vindo à Astrovistorias</h2>
      <p style="color:#374151;font-size:15px;line-height:1.55">
        Olá <strong>{nome}</strong>,<br>
        {criador_nome or 'O administrador'} criou um acesso para você no painel Astrovistorias.
        Clique no botão abaixo para definir sua senha e ativar sua conta.
      </p>
      <p style="text-align:center;margin:28px 0">
        <a href="{link}" style="display:inline-block;background:#0e7490;color:#fff;text-decoration:none;padding:12px 28px;border-radius:8px;font-size:14px;font-weight:700">
          Ativar minha conta
        </a>
      </p>
      <p style="color:#6b7280;font-size:12px;line-height:1.5">
        Se o botão não funcionar, copie e cole este link no navegador:<br>
        <span style="word-break:break-all;color:#0e7490">{link}</span>
      </p>
      <p style="color:#9ca3af;font-size:12px;margin-top:20px">
        Este link expira em {_INVITE_TTL_HOURS} horas e só pode ser usado uma vez.
        Se você não esperava este email, ignore-o.
      </p>
    </div>
    """
    _send_email_to([email], "Seu acesso Astrovistorias", html)


def _send_email_to(recipients: list[str], subject: str, html: str) -> None:
    """Envia email para destinatários específicos (diferente de _send_email que usa ALERT_EMAILS).

    Usado para convites, notificações direcionadas. Levanta RuntimeError se provider
    não configurado (chamador decide como reportar pro usuário).
    """
    if not recipients:
        raise RuntimeError("Sem destinatario.")
    provider = _email_provider()
    if not provider:
        raise RuntimeError("Nenhum provider de email configurado (RESEND_API_KEY ou SMTP_*).")
    if provider == "resend":
        _send_via_resend(subject, html, recipients)
    else:
        _send_via_smtp(subject, html, recipients)


def _send_email(subject: str, html: str, attachment: bytes | None = None, attachment_name: str = "") -> None:
    """Envia email. Prefere Resend (API HTTPS) se RESEND_API_KEY setada; senao SMTP.

    Se nenhum provider estiver configurado, loga warning e nao envia (nao levanta).
    """
    recipients = [e.strip() for e in os.environ.get("ALERT_EMAILS", "").split(",") if e.strip()]
    if not recipients:
        app.logger.warning("[email] ALERT_EMAILS vazio — email nao enviado.")
        return
    provider = _email_provider()
    if not provider:
        app.logger.warning("[email] Nenhum provider configurado (nem RESEND_API_KEY nem SMTP_*) — email nao enviado.")
        return
    if provider == "resend":
        _send_via_resend(subject, html, recipients, attachment, attachment_name)
    else:
        _send_via_smtp(subject, html, recipients, attachment, attachment_name)


def _enviar_alerta_fechamento(today: str) -> None:
    tz   = ZoneInfo("America/Sao_Paulo")
    rows = ""
    tem_movimento = False
    for uid, ud in UNITS.items():
        try:
            lcs = _db_load(uid, _unit_state_dir(uid), today)
        except Exception:
            lcs = []
        nome   = ud.get("nome", uid)
        count  = len(lcs)
        total  = sum(float(lc.get("valor", 0)) for lc in lcs)
        brl    = lambda v: f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if count > 0:
            tem_movimento = True
            status_html = '<span style="color:#d97706;font-weight:700">⚠ Verificar fechamento</span>'
        else:
            status_html = '<span style="color:#6b7280">Sem movimentação</span>'
        rows += f"""
        <tr>
          <td style="padding:10px 16px;border-bottom:1px solid #e5e7eb;font-weight:600">{nome}</td>
          <td style="padding:10px 16px;border-bottom:1px solid #e5e7eb;text-align:center">{count}</td>
          <td style="padding:10px 16px;border-bottom:1px solid #e5e7eb;text-align:right;font-weight:700">{brl(total)}</td>
          <td style="padding:10px 16px;border-bottom:1px solid #e5e7eb">{status_html}</td>
        </tr>"""

    data_fmt = today[8:] + "/" + today[5:7] + "/" + today[:4]
    html = f"""<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">
  <div style="max-width:560px;margin:32px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08)">
    <div style="background:#0f1117;padding:24px 28px">
      <div style="font-size:11px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:rgba(255,255,255,.4);margin-bottom:6px">Astrovistorias · Alerta automático</div>
      <div style="font-size:20px;font-weight:800;color:#fff">Status do Caixa — {data_fmt}</div>
      <div style="font-size:13px;color:rgba(255,255,255,.4);margin-top:4px">Verificação das 18:30 — horário de Brasília</div>
    </div>
    <div style="padding:24px 28px">
      <p style="font-size:13px;color:#6b7280;margin:0 0 16px">Abaixo o status de cada unidade no momento da verificação. Unidades com lançamentos devem ter o fechamento confirmado.</p>
      <table style="width:100%;border-collapse:collapse;font-size:13px">
        <thead>
          <tr style="background:#f9fafb">
            <th style="padding:10px 16px;text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:#9ca3af;border-bottom:2px solid #e5e7eb">Unidade</th>
            <th style="padding:10px 16px;text-align:center;font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:#9ca3af;border-bottom:2px solid #e5e7eb">Lançamentos</th>
            <th style="padding:10px 16px;text-align:right;font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:#9ca3af;border-bottom:2px solid #e5e7eb">Total</th>
            <th style="padding:10px 16px;font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:#9ca3af;border-bottom:2px solid #e5e7eb">Status</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    <div style="padding:16px 28px 24px;border-top:1px solid #f3f4f6">
      <a href="https://astro-v2.up.railway.app/gerencial" style="display:inline-block;background:#3b82f6;color:#fff;text-decoration:none;padding:9px 20px;border-radius:8px;font-size:13px;font-weight:600">Abrir Gerencial Rede</a>
    </div>
  </div>
</body></html>"""

    subject = f"[Astrovistorias] Caixa do Dia — {data_fmt}"
    try:
        _send_email(subject, html)
    except Exception as e:
        app.logger.error("[email] Falha ao enviar alerta: %s", e)


def _criar_backup_zip() -> bytes:
    """Gera um ZIP em memória com dump SQL de todos os bancos + JSONs de config."""
    import io, zipfile, sqlite3
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for uid in UNITS:
            unit_dir = DATA_DIR / uid
            db_path  = unit_dir / "caixa_dia.db"
            if db_path.exists():
                try:
                    conn = sqlite3.connect(str(db_path))
                    sql  = "\n".join(conn.iterdump())
                    conn.close()
                    zf.writestr(f"{uid}/caixa_dia.sql", sql.encode("utf-8"))
                except Exception as exc:
                    app.logger.error("[backup] Falha ao dumpar %s: %s", uid, exc)
            for fname in ("imported.json", "cliente_ids.json"):
                p = unit_dir / fname
                if p.exists():
                    zf.writestr(f"{uid}/{fname}", p.read_bytes())
    buf.seek(0)
    return buf.read()


def _executar_backup() -> None:
    tz      = ZoneInfo("America/Sao_Paulo")
    today   = dt.datetime.now(tz).date().isoformat()
    data_fmt = today[8:] + "/" + today[5:7] + "/" + today[:4]
    app.logger.info("[backup] Iniciando backup de %d unidade(s)", len(UNITS))
    try:
        zip_bytes = _criar_backup_zip()
        size_kb   = len(zip_bytes) // 1024
        html = f"""<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="UTF-8"></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f3f4f6;margin:0;padding:0">
  <div style="max-width:520px;margin:32px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08)">
    <div style="background:#0f1117;padding:24px 28px">
      <div style="font-size:11px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:rgba(255,255,255,.4);margin-bottom:6px">Astrovistorias · Backup Automático</div>
      <div style="font-size:20px;font-weight:800;color:#fff">Backup do Sistema — {data_fmt}</div>
    </div>
    <div style="padding:24px 28px;font-size:14px;color:#374151">
      <p>Backup diário concluído com sucesso.</p>
      <ul>
        <li><strong>Unidades:</strong> {', '.join(UNITS.keys())}</li>
        <li><strong>Tamanho:</strong> {size_kb} KB</li>
        <li><strong>Conteúdo:</strong> dump SQL de cada banco + arquivos de configuração</li>
      </ul>
      <p style="color:#6b7280;font-size:12px">Para restaurar: <code>sqlite3 novo.db &lt; caixa_dia.sql</code></p>
    </div>
  </div>
</body></html>"""
        fname = f"backup_astro_{today}.zip"
        _send_email(f"[Astrovistorias] Backup {data_fmt}", html, zip_bytes, fname)
        app.logger.info("[backup] Concluido — %d KB enviados", size_kb)
    except Exception as exc:
        app.logger.error("[backup] Falha: %s", exc)


def _enviar_email_envio_tiny(unit: str, results: dict, records: list) -> None:
    """Dispara email de confirmacao apos envio ao Tiny concluir.

    Resumo: quantos enviados / pulados / falhas + total em R$ enviado.
    Falha silenciosa (log) — nao trava a response do envio.
    """
    try:
        unit_nome = UNITS.get(unit, {}).get("nome", unit)
        tz        = ZoneInfo("America/Sao_Paulo")
        data_fmt  = dt.datetime.now(tz).strftime("%d/%m/%Y %H:%M")
        enviados  = len(results.get("enviados", []))
        pulados   = len(results.get("pulados", []))
        falhas    = len(results.get("falhas", []))

        # Total enviado em R$. Tenta casar por id; se nao bater (id regenerado
        # em _process_one), cai em heuristica: soma proporcional dos records
        # pelo numero de enviados. Evita mostrar "R$ 0,00" quando houve envio.
        chaves_enviadas = {e["chave"] for e in results.get("enviados", [])}
        total_valor     = sum(float(r.get("preco", 0) or 0) for r in records
                              if r.get("id") in chaves_enviadas)
        if total_valor == 0 and enviados > 0 and records:
            soma_todos = sum(float(r.get("preco", 0) or 0) for r in records)
            total_valor = soma_todos * enviados / len(records)
        brl = f"R$ {total_valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        if enviados == 0 and falhas == 0:
            return   # tudo pulado (ja importado) — nao faz sentido alertar

        cor_status = "#16a34a" if falhas == 0 else "#d97706"
        status_txt = "Envio concluido" if falhas == 0 else "Envio concluido com falhas"

        falhas_html = ""
        if falhas:
            falhas_html = '<div style="margin-top:16px;padding:12px 14px;background:#fef2f2;border-left:3px solid #dc2626;border-radius:4px;font-size:12.5px;color:#991b1b"><strong>Falhas:</strong><ul style="margin:6px 0 0;padding-left:20px">'
            for f in results["falhas"][:5]:
                falhas_html += f'<li>{f.get("cliente","")} — {f.get("erro","")[:120]}</li>'
            if falhas > 5:
                falhas_html += f'<li>... e mais {falhas - 5}</li>'
            falhas_html += '</ul></div>'

        html = f"""<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">
  <div style="max-width:520px;margin:32px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08)">
    <div style="background:#0f1117;padding:24px 28px">
      <div style="font-size:11px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:rgba(255,255,255,.4);margin-bottom:6px">Astrovistorias · Envio Tiny</div>
      <div style="font-size:20px;font-weight:800;color:#fff">{unit_nome} — {data_fmt}</div>
      <div style="font-size:13px;color:{cor_status};margin-top:6px;font-weight:700">{status_txt}</div>
    </div>
    <div style="padding:20px 28px">
      <table style="width:100%;border-collapse:collapse;font-size:13px">
        <tr><td style="padding:6px 0;color:#6b7280">Enviados ao Tiny</td><td style="padding:6px 0;text-align:right;font-weight:700;color:#111">{enviados}</td></tr>
        <tr><td style="padding:6px 0;color:#6b7280">Pulados (duplicatas)</td><td style="padding:6px 0;text-align:right;font-weight:700;color:#6b7280">{pulados}</td></tr>
        <tr><td style="padding:6px 0;color:#6b7280">Falhas</td><td style="padding:6px 0;text-align:right;font-weight:700;color:{'#dc2626' if falhas else '#6b7280'}">{falhas}</td></tr>
        <tr><td style="padding:10px 0 6px;border-top:1px solid #e5e7eb;color:#111;font-weight:700">Total enviado</td><td style="padding:10px 0 6px;border-top:1px solid #e5e7eb;text-align:right;font-weight:800;color:#111;font-size:15px">{brl}</td></tr>
      </table>
      {falhas_html}
    </div>
    <div style="padding:0 28px 24px">
      <a href="https://astro-v2.up.railway.app/u/{unit}/gerencial" style="display:inline-block;background:#3b82f6;color:#fff;text-decoration:none;padding:9px 20px;border-radius:8px;font-size:13px;font-weight:600">Ver historico da unidade</a>
    </div>
  </div>
</body></html>"""
        subject = f"[Astrovistorias] Envio Tiny — {unit_nome} ({enviados} enviados)"
        _send_email(subject, html)
    except Exception as e:
        app.logger.warning("[email:envio_tiny] falha: %s", e)


def _verificar_saude_tokens() -> None:
    """Tenta renovar o access_token de cada unidade preventivamente.

    Se a renovacao falhar (refresh_token expirado/invalido), manda email
    imediato pro Ian com link de reautorizacao. Deduplicacao: o arquivo
    `.token_alert_sent` no dir da unidade marca que ja foi alertado hoje.
    """
    tz    = ZoneInfo("America/Sao_Paulo")
    today = dt.datetime.now(tz).date().isoformat()
    for uid in UNITS:
        try:
            config    = _build_unit_config(uid)
            state_dir = _unit_state_dir(uid)
            _seed_tokens(uid, config)
            importer  = TinyImporter(config, state_dir)
            # Forca renovacao; se funcionar, access_token fica valido 4h+
            importer.refresh_access_token()
            app.logger.info("[cron:tokens] %s OK", uid)
        except Exception as exc:
            # Deduplica alerta por unidade por dia
            marker = _unit_state_dir(uid) / ".token_alert_sent"
            if marker.exists() and marker.read_text().strip() == today:
                app.logger.info("[cron:tokens] %s falhou mas ja alertou hoje", uid)
                continue
            app.logger.warning("[cron:tokens] %s falhou: %s", uid, exc)
            unit_nome = UNITS.get(uid, {}).get("nome", uid)
            html = f"""<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">
  <div style="max-width:520px;margin:32px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08)">
    <div style="background:#991b1b;padding:24px 28px">
      <div style="font-size:11px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:rgba(255,255,255,.6);margin-bottom:6px">Astrovistorias · Atencao</div>
      <div style="font-size:20px;font-weight:800;color:#fff">Token Tiny expirado — {unit_nome}</div>
    </div>
    <div style="padding:20px 28px;font-size:14px;color:#374151;line-height:1.55">
      <p>O token da unidade <strong>{unit_nome}</strong> nao conseguiu renovar. O fechamento nao vai funcionar ate reautorizar.</p>
      <p style="color:#6b7280;font-size:12.5px;margin-top:14px">Detalhe tecnico: <code style="background:#f3f4f6;padding:2px 6px;border-radius:3px">{str(exc)[:200]}</code></p>
    </div>
    <div style="padding:0 28px 24px">
      <a href="https://astro-v2.up.railway.app/u/{uid}/gerencial" style="display:inline-block;background:#dc2626;color:#fff;text-decoration:none;padding:10px 22px;border-radius:8px;font-size:13px;font-weight:700">Reautorizar agora</a>
    </div>
  </div>
</body></html>"""
            try:
                _send_email(f"[Astrovistorias] Token Tiny expirado — {unit_nome}", html)
                marker.write_text(today)
            except Exception as email_exc:
                app.logger.error("[cron:tokens] falha ao enviar email de alerta: %s", email_exc)


def _cron_loop() -> None:
    tz           = ZoneInfo("America/Sao_Paulo")
    last_alerta  = ""
    last_backup  = ""
    last_tokens  = ""
    while True:
        try:
            now   = dt.datetime.now(tz)
            today = now.date().isoformat()
            if now.hour == 18 and now.minute == 30 and last_alerta != today:
                last_alerta = today
                app.logger.info("[cron] Alerta de fechamento para %s", today)
                _enviar_alerta_fechamento(today)
            if now.hour == 0 and now.minute == 0 and last_backup != today:
                last_backup = today
                _executar_backup()
            if now.hour == 8 and now.minute == 0 and last_tokens != today:
                last_tokens = today
                app.logger.info("[cron] Health check dos tokens Tiny")
                _verificar_saude_tokens()
        except Exception:
            app.logger.exception("[cron] Erro no loop")
        time.sleep(60)


threading.Thread(target=_cron_loop, daemon=True, name="cron").start()


# ══════════════════════════════════════════════════════════════════════════════
# Rota: backup manual (master only)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/gerencial/api/backup", methods=["POST"])
@master_only_required
@csrf_required
def api_backup_manual():
    """Dispara backup imediato e envia por email. Retorna tamanho do ZIP."""
    try:
        zip_bytes = _criar_backup_zip()
        size_kb   = len(zip_bytes) // 1024
        tz        = ZoneInfo("America/Sao_Paulo")
        today     = dt.datetime.now(tz).date().isoformat()
        data_fmt  = today[8:] + "/" + today[5:7] + "/" + today[:4]
        html = f"<p>Backup manual disparado em {data_fmt}. Tamanho: {size_kb} KB.</p>"
        fname = f"backup_astro_{today}_manual.zip"
        _send_email(f"[Astrovistorias] Backup Manual {data_fmt}", html, zip_bytes, fname)
        return _json({"success": True, "size_kb": size_kb, "message": f"Backup enviado por email ({size_kb} KB)."})
    except Exception as exc:
        app.logger.exception("[backup] Falha no backup manual")
        return _json({"success": False, "error": str(exc)}, 500)


# ══════════════════════════════════════════════════════════════════════════════
# Ponto de entrada
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
