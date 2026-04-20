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

from flask import Flask, Response, redirect, request, send_from_directory, session, url_for

from caixa_helpers import FP_VALIDOS, calcular_totais, validar_lancamento
from caixa_db import (migrate_from_json as _db_migrate, load_lancamentos as _db_load,
                       _connect as _db_connect, load_lancamentos_range as _db_load_range,
                       insert_divergencia as _db_insert_div, load_divergencias_range as _db_load_div)

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
def _load_users() -> dict[str, Any]:
    raw = os.environ.get("USERS_CONFIG", "{}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def _load_units() -> dict[str, Any]:
    raw = os.environ.get("UNITS_CONFIG", "{}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


# Carrega uma vez no startup (variaveis de ambiente nao mudam em runtime)
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


def master_only_required(f):
    """Exige login + master: true (acesso global a todas as unidades)."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        user = _current_user()
        if not user:
            if "/api/" in request.path:
                return _json({"success": False, "error": "Sessao expirada.", "session_expired": True}, 401)
            return redirect(url_for("login_page"))
        if not user.get("master"):
            return Response("Acesso restrito ao master.", status=403)
        return f(*args, **kwargs)
    return wrapper


def gerencial_required(f):
    """Exige login + acesso à unidade + flag gerencial (ou master global)."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        user = _current_user()
        if not user:
            if "/api/" in request.path:
                return _json({"success": False, "error": "Sessao expirada.", "session_expired": True}, 401)
            return redirect(url_for("login_page"))
        unit = kwargs.get("unit")
        if not user.get("master") and user.get("unit") != unit:
            return Response("Acesso negado a esta unidade.", status=403)
        if not user.get("master") and not user.get("gerencial"):
            return Response("Acesso restrito ao gerente da unidade.", status=403)
        return f(*args, **kwargs)
    return wrapper


def unit_access_required(f):
    """Verifica login + acesso a unidade (master ve tudo).
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
        if not user.get("master") and user.get("unit") != unit:
            if "/api/" in request.path:
                return _json({"success": False, "error": "Acesso negado a esta unidade."}, 403)
            return Response("Acesso negado", status=403)
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


def _build_unit_config(unit: str) -> dict[str, Any]:
    """Monta config completo para a unidade a partir de UNITS_CONFIG."""
    ud = UNITS.get(unit, {})
    tiny: dict[str, Any] = {}

    for field in (
        "client_id", "client_secret", "refresh_token", "redirect_uri",
        "forma_recebimento_ids", "cliente_ids", "categoria_id", "categoria_ids",
        "vencimento_dias", "vencimento_tipo", "numero_documento_prefix",
        "auto_create_contacts", "require_payment_mapping",
        "include_forma_recebimento", "default_tipo_pessoa",
    ):
        if (v := ud.get(field)) is not None:
            tiny[field] = v

    if "aliases" in ud:
        tiny["aliases"] = ud["aliases"]

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
    return _json({
        "usuario": session.get("name", ""),
        "email":   session.get("email", ""),
        "master":  bool(user and user.get("master")),
        "gerencial": bool(user and (user.get("gerencial") or user.get("master"))),
        "unit":    user.get("unit", "") if user else "",
    })


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
    if user.get("master"):
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
    if not user.get("master"):
        unit = user.get("unit")
        return redirect(f"/u/{unit}/") if unit else redirect(url_for("login_page"))
    return send_from_directory(UI_DIR, "master.html")


@app.route("/master/api/units")
@master_required
def master_api_units():
    units_info = [
        {"id": uid, "nome": ud.get("nome", uid)}
        for uid, ud in UNITS.items()
    ]
    return _json({"units": units_info})


@app.route("/master/api/units/status")
@master_required
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
        ultima = max((lc.get("timestamp", "") for lc in lancamentos), default=None)
        status.append({
            "id":   uid,
            "nome": ud.get("nome", uid),
            "hoje": {
                "lancamentos":    len(lancamentos),
                "total":          totais["total"],
                "ultima_atividade": ultima,
            },
        })
    return _json({"status": status, "data": today})


@app.route("/master/api/tiny-health")
@master_required
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
        "gerencial": bool(user and (user.get("gerencial") or user.get("master"))),
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
                    results["pulados"].append({"chave": rec.chave_deduplicacao, "cliente": rec.cliente, "motivo": "ja importado"})
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
                    results["enviados"].append({"chave": rec.chave_deduplicacao, "cliente": rec.cliente})
            except Exception as exc:
                with lock:
                    if _is_doc_already_registered(exc):
                        imported[rec.chave_deduplicacao] = {
                            "arquivo": rec.origem_arquivo,
                            "linha": rec.linha_origem,
                            "enviado_em": dt.datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds"),
                            "motivo": "ja existia no Tiny (numeroDocumento duplicado)",
                        }
                        results["pulados"].append({"chave": rec.chave_deduplicacao, "cliente": rec.cliente, "motivo": "ja existia no Tiny"})
                    else:
                        app.logger.exception("[send] falha chave=%s cliente=%s", rec.chave_deduplicacao, rec.cliente)
                        results["falhas"].append({
                            "chave": rec.chave_deduplicacao,
                            "cliente": rec.cliente,
                            "erro": str(exc),
                        })

        # Processa todos os registros do lote em paralelo (5 threads concorrentes)
        # Cada thread faz chamadas de I/O ao Tiny de forma independente
        from concurrent.futures import ThreadPoolExecutor
        records = data.get("records", [])
        with ThreadPoolExecutor(max_workers=5) as pool:
            list(pool.map(_process_one, records))

        # Salva estado uma unica vez apos processar todos (escrita atomica)
        save_state(state_path, st)

        return _json({
            "success": True, "summary": results,
            "message": f"Processamento concluido. Enviados: {len(results['enviados'])}",
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

    _KEEP = {"id", "hora", "timestamp", "placa", "cliente", "cpf", "servico", "valor", "fp"}
    lancamentos = [
        {k: v for k, v in lc.items() if k in _KEEP}
        for lc in _db_load(unit, unit_dir, today)
    ]
    return {"data": today, "lancamentos": lancamentos}


def _save_caixa_dia(unit: str, state: dict[str, Any]) -> None:
    unit_dir = _unit_state_dir(unit)
    today    = state["data"]
    lcs      = state["lancamentos"]
    with _db_connect(unit_dir) as conn:
        conn.execute("DELETE FROM lancamentos WHERE unit=? AND data=?", (unit, today))
        if lcs:
            conn.executemany(
                "INSERT INTO lancamentos "
                "(id,unit,data,hora,timestamp,placa,cliente,cpf,servico,valor,fp) "
                "VALUES (:id,:unit,:data,:hora,:timestamp,:placa,:cliente,:cpf,:servico,:valor,:fp)",
                [{**lc, "unit": unit, "data": today, "cpf": lc.get("cpf", "")} for lc in lcs],
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


@app.route("/u/<unit>/api/caixa/estado")
@unit_access_required
def api_caixa_estado(unit: str):
    try:
        state = _load_caixa_dia(unit)
        return _json({
            "success": True,
            "data": state["data"],
            "lancamentos": state["lancamentos"],
            "totais": _caixa_totals(state["lancamentos"]),
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

        err = validar_lancamento({"placa": placa, "cliente": cliente, "servico": servico,
                                   "valor": valor, "fp": fp})
        if err:
            return _json({"success": False, "error": err}, 400)

        now = dt.datetime.now(ZoneInfo("America/Sao_Paulo"))
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
        }
        state = _load_caixa_dia(unit)
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
                lc.update({"placa": placa, "cliente": cliente, "cpf": cpf,
                            "servico": servico, "valor": round(valor, 2), "fp": fp})
                _save_caixa_dia(unit, state)
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
        state["lancamentos"] = [lc for lc in state["lancamentos"] if lc["id"] != lancamento_id]
        if len(state["lancamentos"]) == antes:
            return _json({"success": False, "error": "Lancamento nao encontrado."}, 404)

        _save_caixa_dia(unit, state)
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
      "ok"               — placa+servico encontrado no PDV, valor igual
      "divergencia_valor" — encontrado no PDV mas valor difere
      "sem_pdv"          — placa+servico nao encontrado no PDV hoje
    """
    try:
        import re
        import unicodedata as _ud

        data    = request.get_json(force=True, silent=True) or {}
        records = data.get("records", [])
        config  = _build_unit_config(unit)
        caixa   = _load_caixa_dia(unit)
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

        # Chaves da planilha (AV e FA) para detectar PDV sem planilha
        planilha_keys: set[tuple] = set()
        conferencia: dict[str, dict] = {}
        for r in records:
            planilha_fp = r.get("fp", "AV")   # "AV" ou "FA"
            rec_id  = r.get("id", "")
            placa   = _norm_placa(r.get("placa", ""))
            servico = _norm_servico(r.get("servico", ""))
            preco   = float(r.get("preco", 0))
            planilha_keys.add((placa, servico))

            pdv_key = _find_pdv_key(placa, servico)
            if pdv_key is None:
                conferencia[rec_id] = {
                    "status": "sem_pdv",
                    "pdv_valor": None,
                    "pdv_fp": None,
                    "pdv_hora": None,
                }
            else:
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

        # Lançamentos do PDV sem nenhum correspondente na planilha (AV ou FA)
        # — serviços avulsos: PESQUISA AVULSA, BAIXA PERMANENTE, faturados sem planilha etc.
        pdv_sem_planilha = []
        for (placa_key, servico_key), lc in pdv_map.items():
            if not _planilha_has_match(placa_key, servico_key, planilha_keys):
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
    try:
        date_from = request.args.get("from", "")
        date_to   = request.args.get("to", "")
        dt.date.fromisoformat(date_from)
        dt.date.fromisoformat(date_to)

        unit_dir    = _unit_state_dir(unit)
        lancamentos = _db_load_range(unit, unit_dir, date_from, date_to)

        fp_keys = ("dinheiro", "debito", "credito", "pix", "faturado", "detran")
        totais: dict[str, float] = {fp: 0.0 for fp in fp_keys}
        for lc in lancamentos:
            fp = lc.get("fp", "")
            if fp in totais:
                totais[fp] += float(lc.get("valor", 0))

        total  = sum(totais.values())
        avista = total - totais["faturado"] - totais["detran"]
        count  = len(lancamentos)

        # Agrupa por dia
        by_day: dict[str, list] = {}
        for lc in lancamentos:
            by_day.setdefault(lc["data"], []).append(lc)

        por_dia = []
        for data in sorted(by_day.keys()):
            dlcs = by_day[data]
            dt_fp: dict[str, float] = {fp: 0.0 for fp in fp_keys}
            for lc in dlcs:
                fp = lc.get("fp", "")
                if fp in dt_fp:
                    dt_fp[fp] += float(lc.get("valor", 0))
            dtotal = sum(dt_fp.values())
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
                "lancamentos": dlcs,
            })

        # Ranking de serviços
        svc_count: dict[str, int]   = {}
        svc_total: dict[str, float] = {}
        for lc in lancamentos:
            s = lc.get("servico", "").strip()
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

        unit_dir    = _unit_state_dir(unit)
        lancamentos = _db_load_range(unit, unit_dir, date_from, date_to)

        out = io.StringIO()
        w   = csv.writer(out)
        w.writerow(["Data", "Hora", "Placa", "Cliente", "CPF/CNPJ", "Servico", "Valor", "FP"])
        for lc in lancamentos:
            w.writerow([
                lc.get("data", ""), lc.get("hora", ""), lc.get("placa", ""),
                lc.get("cliente", ""), lc.get("cpf", ""), lc.get("servico", ""),
                lc.get("valor", ""), lc.get("fp", ""),
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
@master_only_required
def master_gerencial_page():
    return _nocache(send_from_directory(UI_DIR, "master_gerencial.html"))


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


@app.route("/gerencial/api/historico")
@master_only_required
def api_master_historico():
    try:
        date_from   = request.args.get("from", "")
        date_to     = request.args.get("to", "")
        unit_filter = request.args.get("unit", "all")
        dt.date.fromisoformat(date_from)
        dt.date.fromisoformat(date_to)

        fp_keys = ("dinheiro", "debito", "credito", "pix", "faturado", "detran")
        units_to_query = list(UNITS.keys()) if unit_filter == "all" else (
            [unit_filter] if unit_filter in UNITS else []
        )

        all_lcs: list[dict] = []
        por_unidade = []
        for uid in units_to_query:
            ud = UNITS[uid]
            try:
                lcs = _db_load_range(uid, _unit_state_dir(uid), date_from, date_to)
            except Exception:
                lcs = []
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
            by_day.setdefault(lc["data"], []).append(lc)

        por_dia = []
        for data in sorted(by_day.keys()):
            dlcs = by_day[data]
            dagg = _agg_lancamentos(dlcs, fp_keys)
            dt_fp: dict[str, float] = {fp: 0.0 for fp in fp_keys}
            for lc in dlcs:
                fp = lc.get("fp", "")
                if fp in dt_fp:
                    dt_fp[fp] += float(lc.get("valor", 0))
            por_dia.append({"data": data, **dagg,
                            **{fp: round(dt_fp[fp], 2) for fp in fp_keys},
                            "lancamentos": dlcs})

        # Ranking serviços
        svc_count: dict[str, int]   = {}
        svc_total: dict[str, float] = {}
        for lc in all_lcs:
            s = lc.get("servico", "").strip()
            if s:
                svc_count[s] = svc_count.get(s, 0) + 1
                svc_total[s] = svc_total.get(s, 0.0) + float(lc.get("valor", 0))
        servicos = sorted(
            [{"servico": s, "count": svc_count[s], "total": round(svc_total[s], 2)}
             for s in svc_count],
            key=lambda x: x["count"], reverse=True,
        )

        return _json({
            "success":      True,
            "unidades":     {uid: UNITS[uid].get("nome", uid) for uid in UNITS},
            "unit_filter":  unit_filter,
            "periodo":      {"from": date_from, "to": date_to},
            "resumo":       resumo,
            "por_unidade":  por_unidade,
            "por_dia":      por_dia,
            "servicos":     servicos,
        })
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        app.logger.exception("[server] %s", request.path)
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/gerencial/api/exportar")
@master_only_required
def api_master_exportar():
    try:
        import csv, io
        date_from   = request.args.get("from", "")
        date_to     = request.args.get("to", "")
        unit_filter = request.args.get("unit", "all")
        dt.date.fromisoformat(date_from)
        dt.date.fromisoformat(date_to)

        fp_keys = ("dinheiro", "debito", "credito", "pix", "faturado", "detran")
        units_to_query = list(UNITS.keys()) if unit_filter == "all" else (
            [unit_filter] if unit_filter in UNITS else []
        )
        all_lcs: list[dict] = []
        for uid in units_to_query:
            try:
                lcs = _db_load_range(uid, _unit_state_dir(uid), date_from, date_to)
                for lc in lcs:
                    lc["unit_nome"] = UNITS[uid].get("nome", uid)
                all_lcs.extend(lcs)
            except Exception:
                pass
        all_lcs.sort(key=lambda x: (x.get("data", ""), x.get("timestamp", "")))

        out = io.StringIO()
        w   = csv.writer(out)
        w.writerow(["Unidade", "Data", "Hora", "Placa", "Cliente", "CPF/CNPJ", "Servico", "Valor", "FP"])
        for lc in all_lcs:
            w.writerow([
                lc.get("unit_nome", ""), lc.get("data", ""), lc.get("hora", ""),
                lc.get("placa", ""), lc.get("cliente", ""), lc.get("cpf", ""),
                lc.get("servico", ""), lc.get("valor", ""), lc.get("fp", ""),
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
@master_only_required
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

def _send_email(subject: str, html: str, attachment: bytes | None = None, attachment_name: str = "") -> None:
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText as _MIMEText
    from email.mime.base import MIMEBase
    from email import encoders
    host       = os.environ.get("SMTP_HOST", "")
    port       = int(os.environ.get("SMTP_PORT", "465"))
    user       = os.environ.get("SMTP_USER", "")
    passwd     = os.environ.get("SMTP_PASS", "")
    recipients = [e.strip() for e in os.environ.get("ALERT_EMAILS", "").split(",") if e.strip()]
    if not all([host, user, passwd, recipients]):
        app.logger.warning("[email] SMTP nao configurado — email nao enviado.")
        return
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
    app.logger.info("[email] Enviado: %s → %s", subject, recipients)


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


def _cron_loop() -> None:
    tz           = ZoneInfo("America/Sao_Paulo")
    last_alerta  = ""
    last_backup  = ""
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
