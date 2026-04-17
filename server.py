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
      "name": "Nome do Usuario"
    }
  }

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
    PERMANENT_SESSION_LIFETIME=43200,  # 12 horas
)

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
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
    return _json({"success": False, "error": str(exc), "traceback": tb}, 500)

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


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not _current_user():
            return redirect(url_for("login_page"))
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
    """Semeie o refresh_token de UNITS_CONFIG no arquivo de tokens se ainda nao existir."""
    p = _unit_state_dir(unit) / "tiny_tokens.json"
    if p.exists():
        return
    rt = config["tiny"].get("refresh_token", "")
    if rt:
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
# Rotas: autenticacao
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "POST":
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
        return redirect(url_for("master_page"))
    unit = user.get("unit")
    if unit:
        return redirect(f"/u/{unit}/")
    return redirect(url_for("login_page"))


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
@login_required
def master_api_units():
    user = _current_user()
    if not user.get("master"):
        return _json({"error": "Acesso negado"}, 403)
    units_info = [
        {"id": uid, "nome": ud.get("nome", uid)}
        for uid, ud in UNITS.items()
    ]
    return _json({"units": units_info})


# ══════════════════════════════════════════════════════════════════════════════
# Rotas: arquivos estaticos da frente de caixa
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/u/<unit>/")
@unit_access_required
def unit_index(unit: str):
    return send_from_directory(UI_DIR, "index.html")


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
    # Detecta automaticamente a URL de redirecionamento baseada no host atual
    config = _build_unit_config(unit)
    tiny = config["tiny"]
    redirect_uri = f"https://{request.host}/u/{unit}/callback"
    
    params = {
        "client_id": tiny["client_id"],
        "redirect_uri": redirect_uri,
        "scope": tiny["oauth_scope"],
        "response_type": "code",
    }
    url = f"{tiny['auth_url']}?{urllib.parse.urlencode(params)}"
    return redirect(url)


@app.route("/u/<unit>/callback")
def api_auth_callback(unit: str):
    """Recebe o code do Tiny e troca pelo refresh_token."""
    code = request.args.get("code")
    if not code:
        return Response("Code ausente", status=400)

    config = _build_unit_config(unit)
    state_dir = _unit_state_dir(unit)
    importer = TinyImporter(config, state_dir)
    
    # Detecta a mesma URL dinâmica para a troca do code
    redirect_uri = f"https://{request.host}/u/{unit}/callback"

    try:
        # Troca o code pelos tokens e salva no tiny_tokens.json (no Volume /data)
        importer.client.exchange_authorization_code(code, redirect_uri)
        return f"<h1>Autenticacao concluida!</h1><p>Unidade {unit} autorizada com sucesso. Pode fechar esta aba e voltar ao app.</p>"
    except Exception as exc:
        return f"<h1>Erro na autenticacao:</h1><pre>{exc}</pre>"

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
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/send", methods=["POST"])
@unit_access_required
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
            )
            if rec.chave_deduplicacao == "missing_key" or "-" in rec.chave_deduplicacao:
                rec.chave_deduplicacao = record_key(asdict(rec))

            # Camada 1: check local (thread-safe via lock)
            with lock:
                if rec.chave_deduplicacao in imported:
                    results["pulados"].append({"chave": rec.chave_deduplicacao, "cliente": rec.cliente, "motivo": "ja importado"})
                    return

            try:
                resp = importer.create_accounts_receivable(rec)
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
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/clear-imported", methods=["POST"])
@unit_access_required
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
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/suggest-clients", methods=["POST"])
@unit_access_required
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
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/map-client", methods=["POST"])
@unit_access_required
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
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/auto-map-clients", methods=["POST"])
@unit_access_required
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
        return _json({"success": False, "error": str(exc)}, 500)


# ══════════════════════════════════════════════════════════════════════════════
# Helpers: caixa do dia (PDV)
# ══════════════════════════════════════════════════════════════════════════════

def _load_caixa_dia(unit: str) -> dict[str, Any]:
    p = _unit_state_dir(unit) / "caixa_dia.json"
    today = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).date().isoformat()
    if p.exists():
        try:
            data = json.loads(p.read_text())
            if data.get("data") == today:
                return data
        except Exception:
            pass
    return {"data": today, "lancamentos": []}


def _save_caixa_dia(unit: str, state: dict[str, Any]) -> None:
    p = _unit_state_dir(unit) / "caixa_dia.json"
    tmp = p.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2))
        tmp.replace(p)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _verify_unit_pin(unit: str, pin: str) -> bool:
    stored = str(UNITS.get(unit, {}).get("master_pin", ""))
    return bool(stored) and secrets.compare_digest(pin.strip(), stored)


def _caixa_totals(lancamentos: list[dict]) -> dict[str, Any]:
    totals: dict[str, float] = {"dinheiro": 0.0, "debito": 0.0, "credito": 0.0, "pix": 0.0, "faturado": 0.0}
    for lc in lancamentos:
        fp = lc.get("fp", "")
        if fp in totals:
            totals[fp] += float(lc.get("valor", 0))
    totals["total"] = sum(totals.values())
    totals["total_avista"] = totals["total"] - totals["faturado"]
    return totals


# ══════════════════════════════════════════════════════════════════════════════
# Rotas: caixa do dia (PDV)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/u/<unit>/caixa")
@unit_access_required
def unit_caixa(unit: str):
    return send_from_directory(UI_DIR, "caixa.html")


@app.route("/u/<unit>/caixa2")
@unit_access_required
def unit_caixa2(unit: str):
    return send_from_directory(UI_DIR, "caixa2.html")


@app.route("/u/<unit>/fechamento")
@unit_access_required
def unit_fechamento(unit: str):
    return send_from_directory(UI_DIR, "fechamento.html")


@app.route("/u/<unit>/api/astro", methods=["POST"])
@unit_access_required
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
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/caixa/estado")
@unit_access_required
def api_caixa_estado(unit: str):
    state = _load_caixa_dia(unit)
    return _json({
        "success": True,
        "data": state["data"],
        "lancamentos": state["lancamentos"],
        "totais": _caixa_totals(state["lancamentos"]),
    })


@app.route("/u/<unit>/api/caixa/lancar", methods=["POST"])
@unit_access_required
def api_caixa_lancar(unit: str):
    try:
        data    = request.get_json(force=True, silent=True) or {}
        placa   = clean_text(data.get("placa", "")).upper()
        cliente = clean_text(data.get("cliente", ""))
        servico = clean_text(data.get("servico", "")).upper()
        valor   = float(data.get("valor", 0))
        fp      = data.get("fp", "")

        if not placa:
            return _json({"success": False, "error": "Placa obrigatoria."}, 400)
        if not cliente:
            return _json({"success": False, "error": "Cliente obrigatorio."}, 400)
        if not servico:
            return _json({"success": False, "error": "Servico obrigatorio."}, 400)
        if valor <= 0:
            return _json({"success": False, "error": "Valor deve ser maior que zero."}, 400)
        if fp not in ("dinheiro", "debito", "credito", "pix", "faturado"):
            return _json({"success": False, "error": "Forma de pagamento invalida."}, 400)

        now = dt.datetime.now(ZoneInfo("America/Sao_Paulo"))
        lancamento = {
            "id": secrets.token_hex(8),
            "hora": now.strftime("%H:%M"),
            "timestamp": now.isoformat(),
            "placa": placa,
            "cliente": cliente,
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
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/caixa/editar/<lancamento_id>", methods=["PUT"])
@unit_access_required
def api_caixa_editar(unit: str, lancamento_id: str):
    try:
        data = request.get_json(force=True, silent=True) or {}
        if not _verify_unit_pin(unit, data.get("pin", "")):
            return _json({"success": False, "error": "PIN incorreto."}, 403)

        placa   = clean_text(data.get("placa", "")).upper()
        cliente = clean_text(data.get("cliente", ""))
        servico = clean_text(data.get("servico", "")).upper()
        valor   = float(data.get("valor", 0))
        fp      = data.get("fp", "")

        if not all([placa, cliente, servico]) or valor <= 0 or fp not in ("dinheiro", "debito", "credito", "pix"):
            return _json({"success": False, "error": "Dados invalidos."}, 400)

        state = _load_caixa_dia(unit)
        for lc in state["lancamentos"]:
            if lc["id"] == lancamento_id:
                lc.update({"placa": placa, "cliente": cliente, "servico": servico,
                            "valor": round(valor, 2), "fp": fp})
                _save_caixa_dia(unit, state)
                return _json({"success": True, "totais": _caixa_totals(state["lancamentos"])})

        return _json({"success": False, "error": "Lancamento nao encontrado."}, 404)
    except Exception as exc:
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/caixa/excluir/<lancamento_id>", methods=["DELETE"])
@unit_access_required
def api_caixa_excluir(unit: str, lancamento_id: str):
    try:
        data = request.get_json(force=True, silent=True) or {}
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
        return _json({"success": False, "error": str(exc)}, 500)


@app.route("/u/<unit>/api/caixa/conferir", methods=["POST"])
@unit_access_required
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

        # Chaves da planilha AV (para detectar PDV sem planilha)
        planilha_keys: set[tuple] = set()
        conferencia: dict[str, dict] = {}
        for r in records:
            if r.get("fp") != "AV":
                continue  # apenas AV precisa de cruzamento com PDV

            rec_id  = r.get("id", "")
            placa   = _norm_placa(r.get("placa", ""))
            servico = _norm_servico(r.get("servico", ""))
            preco   = float(r.get("preco", 0))
            key     = (placa, servico)
            planilha_keys.add(key)

            if key not in pdv_map:
                conferencia[rec_id] = {
                    "status": "sem_pdv",
                    "pdv_valor": None,
                    "pdv_fp": None,
                    "pdv_hora": None,
                }
            else:
                lc        = pdv_map[key]
                pdv_valor = float(lc.get("valor", 0))
                status    = "ok" if abs(pdv_valor - preco) < 0.01 else "divergencia_valor"
                conferencia[rec_id] = {
                    "status": status,
                    "pdv_valor": pdv_valor,
                    "pdv_fp": lc.get("fp"),
                    "pdv_hora": lc.get("hora"),
                }

        # Lançamentos do PDV que nao aparecem em nenhum registro AV da planilha
        # (servicos que nunca vem na planilha: PESQUISA AVULSA, BAIXA PERMANENTE etc.)
        pdv_sem_planilha = []
        for (placa_key, servico_key), lc in pdv_map.items():
            if (placa_key, servico_key) not in planilha_keys:
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
        return _json({"success": False, "error": str(exc)}, 500)


# ══════════════════════════════════════════════════════════════════════════════
# Ponto de entrada
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
