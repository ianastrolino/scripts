#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import csv
import datetime as dt
import hashlib
import html
import http.server
import json
import os
import re
import secrets
import shutil
import sys
import time
import unicodedata
import urllib.parse
import webbrowser
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from html.parser import HTMLParser
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent

OUTPUT_FIELDS = [
    "data",
    "modelo",
    "placa",
    "cliente",
    "servico",
    "fp",
    "preco",
    "origem_arquivo",
    "linha_origem",
    "chave_deduplicacao",
]

DEFAULT_CONFIG: dict[str, Any] = {
    "input_dir": "entrada",
    "output_dir": "saida",
    "archive_dir": "processados",
    "state_dir": "state",
    "logs_dir": "logs",
        "tiny": {
        "auth_url": "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/auth",
        "base_url": "https://api.tiny.com.br/public-api/v3",
        "token_url": "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token",
        "oauth_scope": "openid",
        "timeout_seconds": 30,
        "auto_create_contacts": False,
        "require_payment_mapping": False,
        "include_forma_recebimento": True,
        "default_tipo_pessoa": "J",
        "categoria_id": None,
        "categoria_ids": {},
        "contas_receber_fp": ["FA"],
        "vencimento_tipo": "ultimo_dia_mes",
        "vencimento_dias": 0,
        "numero_documento_prefix": "PLANILHA",
        "aliases": {
            "servico": {
                "LAUDO DE VERIFICACA": "LAUDO DE VERIFICACAO",
                "LAUDO CAUTELAR VERI": "LAUDO CAUTELAR VERIFICACAO",
                "CAUTELAR COM ANALIS": "CAUTELAR COM ANALISE",
                "LAUDO DE TRANSFEREN": "LAUDO DE TRANSFERENCIA",
            },
            "fp": {},
            "cliente": {},
        },
        "cliente_ids": {},
        "forma_recebimento_ids": {},
    },
    "server": {
        "port": 8081,
        "host": "localhost",
    },
}


class ImportErrorWithContext(Exception):
    pass


class TinyApiError(Exception):
    pass


def _is_doc_already_registered(exc: Exception) -> bool:
    """Retorna True se o Tiny rejeitou por numeroDocumento duplicado."""
    return "já cadastrado no sistema" in str(exc) or "ja cadastrado no sistema" in str(exc)


def resolve_categoria_id(config: dict[str, Any], servico: str) -> int | None:
    """Resolve o ID de categoria com base no servico, com fallback para categoria_id global."""
    categoria_ids: dict[str, Any] = config.get("categoria_ids") or {}
    if categoria_ids:
        servico_key = normalize_key(servico)
        for nome, cat_id in categoria_ids.items():
            if normalize_key(str(nome)) in servico_key or servico_key in normalize_key(str(nome)):
                return int(cat_id)
    fallback = config.get("categoria_id")
    return int(fallback) if fallback else None


@dataclass
class NormalizedRecord:
    data: str
    modelo: str
    placa: str
    cliente: str
    servico: str
    fp: str
    preco: str
    origem_arquivo: str
    linha_origem: int
    chave_deduplicacao: str
    av_pagamento: str = ""  # preenchido pela frente de caixa para registros AV (dinheiro/debito/credito/pix)


class HtmlTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            if self._cell is not None:
                self._row.append(clean_text("".join(self._cell)))
            self._cell = []

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._row is not None and self._cell is not None:
            self._row.append(clean_text("".join(self._cell)))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._cell is not None:
                self._row.append(clean_text("".join(self._cell)))
                self._cell = None
            if any(cell.strip() for cell in self._row):
                self.rows.append(self._row)
            self._row = None

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)


def clean_text(value: str) -> str:
    value = html.unescape(value).replace("\xa0", " ")
    return re.sub(r"\s+", " ", value).strip()


def remove_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def normalize_key(value: str) -> str:
    value = remove_accents(clean_text(value)).lower()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def merge_config(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = json.loads(json.dumps(base))
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge_config(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_env_file(path: Path | None) -> None:
    if path is None:
        path = BASE_DIR / ".env"
    if not path.exists():
        return

    with path.open("r", encoding="utf-8") as file:
        for line_number, raw_line in enumerate(file, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                raise ImportErrorWithContext(f"Linha invalida no .env ({path}:{line_number}): {raw_line.rstrip()}")

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                raise ImportErrorWithContext(f"Chave vazia no .env ({path}:{line_number}).")
            if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
                value = value[1:-1]
            os.environ.setdefault(key, value)


def env_bool(name: str) -> bool | None:
    value = os.getenv(name)
    if value is None or value == "":
        return None
    normalized = normalize_key(value)
    if normalized in {"1", "true", "yes", "sim", "s", "on"}:
        return True
    if normalized in {"0", "false", "no", "nao", "n", "off"}:
        return False
    raise ImportErrorWithContext(f"Valor booleano invalido em {name}: {value!r}")


def env_int(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise ImportErrorWithContext(f"Valor inteiro invalido em {name}: {value!r}") from exc


def env_json_dict(name: str) -> dict[str, Any] | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ImportErrorWithContext(f"JSON invalido em {name}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ImportErrorWithContext(f"{name} precisa ser um objeto JSON, por exemplo {{\"AV\": 123}}.")
    return parsed


def env_json_list(name: str) -> list[Any] | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ImportErrorWithContext(f"JSON invalido em {name}: {exc}") from exc
    if not isinstance(parsed, list):
        raise ImportErrorWithContext(f"{name} precisa ser uma lista JSON, por exemplo [\"FA\"].")
    return parsed


def apply_env_overrides(config: dict[str, Any]) -> dict[str, Any]:
    env_config: dict[str, Any] = {"tiny": {}}
    path_vars = {
        "IMPORT_INPUT_DIR": "input_dir",
        "IMPORT_OUTPUT_DIR": "output_dir",
        "IMPORT_ARCHIVE_DIR": "archive_dir",
        "IMPORT_STATE_DIR": "state_dir",
        "IMPORT_LOGS_DIR": "logs_dir",
    }
    for env_name, config_key in path_vars.items():
        value = os.getenv(env_name)
        if value:
            env_config[config_key] = value

    tiny_string_vars = {
        "TINY_AUTH_URL": "auth_url",
        "TINY_BASE_URL": "base_url",
        "TINY_TOKEN_URL": "token_url",
        "TINY_OAUTH_SCOPE": "oauth_scope",
        "TINY_DEFAULT_TIPO_PESSOA": "default_tipo_pessoa",
        "TINY_NUMERO_DOCUMENTO_PREFIX": "numero_documento_prefix",
        "TINY_VENCIMENTO_TIPO": "vencimento_tipo",
        "TINY_CLIENT_ID": "client_id",
        "TINY_CLIENT_SECRET": "client_secret",
        "TINY_REFRESH_TOKEN": "refresh_token",
        "TINY_REDIRECT_URI": "redirect_uri",
    }
    for env_name, config_key in tiny_string_vars.items():
        value = os.getenv(env_name)
        if value:
            env_config["tiny"][config_key] = value

    tiny_int_vars = {
        "TINY_TIMEOUT_SECONDS": "timeout_seconds",
        "TINY_CATEGORIA_ID": "categoria_id",
        "TINY_VENCIMENTO_DIAS": "vencimento_dias",
    }
    for env_name, config_key in tiny_int_vars.items():
        value = env_int(env_name)
        if value is not None:
            env_config["tiny"][config_key] = value

    tiny_bool_vars = {
        "TINY_AUTO_CREATE_CONTACTS": "auto_create_contacts",
        "TINY_REQUIRE_PAYMENT_MAPPING": "require_payment_mapping",
        "TINY_INCLUDE_FORMA_RECEBIMENTO": "include_forma_recebimento",
    }
    for env_name, config_key in tiny_bool_vars.items():
        value = env_bool(env_name)
        if value is not None:
            env_config["tiny"][config_key] = value

    cliente_ids = env_json_dict("TINY_CLIENTE_IDS_JSON")
    if cliente_ids is not None:
        env_config["tiny"]["cliente_ids"] = cliente_ids

    forma_recebimento_ids = env_json_dict("TINY_FORMA_RECEBIMENTO_IDS_JSON")
    if forma_recebimento_ids is not None:
        env_config["tiny"]["forma_recebimento_ids"] = forma_recebimento_ids

    contas_receber_fp = env_json_list("TINY_CONTAS_RECEBER_FP_JSON")
    if contas_receber_fp is not None:
        env_config["tiny"]["contas_receber_fp"] = contas_receber_fp

    categoria_ids = env_json_dict("TINY_CATEGORIA_IDS_JSON")
    if categoria_ids is not None:
        env_config["tiny"]["categoria_ids"] = categoria_ids

    aliases = {
        "servico": env_json_dict("TINY_SERVICO_ALIASES_JSON"),
        "fp": env_json_dict("TINY_FP_ALIASES_JSON"),
        "cliente": env_json_dict("TINY_CLIENTE_ALIASES_JSON"),
    }
    if any(value is not None for value in aliases.values()):
        env_config["tiny"]["aliases"] = {
            key: value for key, value in aliases.items() if value is not None
        }

    return merge_config(config, env_config)


def load_config(path: Path | None) -> dict[str, Any]:
    if path is None:
        path = BASE_DIR / "config.json"
    if not path.exists():
        return apply_env_overrides(DEFAULT_CONFIG)
    with path.open("r", encoding="utf-8") as file:
        return apply_env_overrides(merge_config(DEFAULT_CONFIG, json.load(file)))


def project_path(config: dict[str, Any], key: str) -> Path:
    path = Path(config[key])
    if not path.is_absolute():
        path = BASE_DIR / path
    path.mkdir(parents=True, exist_ok=True)
    return path


def read_export(path: Path) -> str:
    raw = path.read_bytes()
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", errors="replace")


def parse_html_table(path: Path) -> list[list[str]]:
    parser = HtmlTableParser()
    parser.feed(read_export(path))
    return parser.rows


def find_header(rows: list[list[str]]) -> tuple[int, list[str], dict[str, int]]:
    wanted = {"data", "modelo", "placa", "cliente", "servico", "fp", "preco"}
    for index, row in enumerate(rows):
        found: dict[str, int] = {}
        for column_index, header in enumerate(row):
            key = normalize_key(header)
            if key in {"data", "modelo", "placa", "cliente", "servico", "fp", "preco"}:
                found[key] = column_index
        if wanted.issubset(found):
            return index, row, found
    raise ImportErrorWithContext(
        "Nao encontrei as colunas obrigatorias: data, modelo, placa, cliente, servico, fp e preco."
    )


def parse_date(value: str) -> str:
    value = clean_text(value)
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return dt.datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            pass
    raise ValueError(f"data invalida: {value!r}")


def looks_like_date(value: str) -> bool:
    value = clean_text(value)
    return bool(re.match(r"^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$", value) or re.match(r"^\d{4}-\d{1,2}-\d{1,2}$", value))


def parse_money(value: str) -> Decimal:
    original = value
    value = clean_text(value)
    value = re.sub(r"[^\d,.\-]", "", value)
    if not value:
        raise ValueError("preco vazio")
    if "," in value:
        value = value.replace(".", "").replace(",", ".")
    elif value.count(".") > 1:
        head, tail = value.rsplit(".", 1)
        value = head.replace(".", "") + "." + tail
    try:
        return Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except InvalidOperation as exc:
        raise ValueError(f"preco invalido: {original!r}") from exc


def normalize_plate(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", remove_accents(value).upper())


def record_key(record: dict[str, Any]) -> str:
    parts = [
        record["data"],
        normalize_key(record["modelo"]),
        record["placa"],
        normalize_key(record["cliente"]),
        normalize_key(record["servico"]),
        record["preco"],
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:20]


def apply_alias(config: dict[str, Any], field: str, value: str) -> str:
    aliases = config.get("tiny", {}).get("aliases", {}).get(field, {})
    value_key = normalize_key(value)
    for alias_from, alias_to in aliases.items():
        if normalize_key(str(alias_from)) == value_key:
            return clean_text(str(alias_to)).upper()
    return value


def normalize_rows(path: Path, config: dict[str, Any]) -> tuple[list[NormalizedRecord], list[str]]:
    rows = parse_html_table(path)
    if not rows:
        raise ImportErrorWithContext(f"Nenhuma tabela encontrada em {path.name}.")

    header_index, _headers, columns = find_header(rows)
    records: list[NormalizedRecord] = []
    errors: list[str] = []

    for row_number, row in enumerate(rows[header_index + 1 :], start=header_index + 2):
        if not any(clean_text(cell) for cell in row):
            continue
        try:
            raw = {
                name: row[column_index] if column_index < len(row) else ""
                for name, column_index in columns.items()
            }

            if not looks_like_date(raw["data"]):
                filled_important = sum(
                    1
                    for name in ("modelo", "placa", "cliente", "servico", "preco")
                    if clean_text(raw.get(name, ""))
                )
                if filled_important < 4 or normalize_key(row[0] if row else "") in {"subtotal", "total"}:
                    continue

            cliente = apply_alias(config, "cliente", clean_text(raw["cliente"]).upper())
            servico = apply_alias(config, "servico", clean_text(raw["servico"]).upper())
            fp = apply_alias(config, "fp", clean_text(raw["fp"]).upper())

            normalized = {
                "data": parse_date(raw["data"]),
                "modelo": clean_text(raw["modelo"]).upper(),
                "placa": normalize_plate(raw["placa"]),
                "cliente": cliente,
                "servico": servico,
                "fp": fp,
                "preco": str(parse_money(raw["preco"])),
                "origem_arquivo": path.name,
                "linha_origem": row_number,
            }
            normalized["chave_deduplicacao"] = record_key(normalized)
            records.append(NormalizedRecord(**normalized))
        except Exception as exc:
            errors.append(f"Linha {row_number}: {exc}")
    return records, errors


def write_outputs(records: list[NormalizedRecord], output_dir: Path, source: Path) -> dict[str, Path]:
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = f"{source.stem}_{timestamp}"
    csv_path = output_dir / f"padronizado_{stem}.csv"
    json_path = output_dir / f"padronizado_{stem}.json"

    data = [asdict(record) for record in records]
    with csv_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(data)
    with json_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    return {"csv": csv_path, "json": json_path}


def latest_input(input_dir: Path) -> Path:
    files = [
        path
        for path in input_dir.iterdir()
        if path.is_file() and path.suffix.lower() in {".xls", ".html", ".htm"}
    ]
    if not files:
        raise ImportErrorWithContext(f"Nenhum arquivo .xls/.html encontrado em {input_dir}.")
    return max(files, key=lambda path: path.stat().st_mtime)


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"imported": {}}
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(state, file, ensure_ascii=False, indent=2)


def lookup_config_id(mapping: dict[str, Any], name: str) -> int | None:
    normalized = normalize_key(name)
    for key, value in mapping.items():
        if key == name or normalize_key(str(key)) == normalized:
            return int(value)
    return None


def money_as_float(value: str) -> float:
    return float(Decimal(value))


def add_days(date_text: str, days: int) -> str:
    base_date = dt.date.fromisoformat(date_text)
    return (base_date + dt.timedelta(days=days)).isoformat()


def last_day_of_month(date_text: str) -> str:
    base_date = dt.date.fromisoformat(date_text)
    last_day = calendar.monthrange(base_date.year, base_date.month)[1]
    return base_date.replace(day=last_day).isoformat()


def due_date_for_record(record: NormalizedRecord, tiny_config: dict[str, Any]) -> str:
    mode = normalize_key(str(tiny_config.get("vencimento_tipo") or "ultimo_dia_mes"))
    if mode in {"ultimo dia mes", "ultimo dia do mes", "fim mes", "fim do mes", "last day of month"}:
        return last_day_of_month(record.data)
    if mode in {"dias", "days"}:
        return add_days(record.data, int(tiny_config.get("vencimento_dias") or 0))
    if mode in {"mesma data", "data servico", "same day"}:
        return record.data
    raise ValueError(f"TINY_VENCIMENTO_TIPO invalido: {tiny_config.get('vencimento_tipo')!r}")


def similarity_score(a: str, b: str) -> float:
    """Retorna score 0.0–1.0 entre dois nomes normalizados."""
    a = normalize_key(a)
    b = normalize_key(b)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if a in b or b in a:
        return 0.85
    wa = set(a.split())
    wb = set(b.split())
    union = wa | wb
    if not union:
        return 0.0
    return len(wa & wb) / len(union)


def _update_env_cliente_id(env_path: Path, nome: str, tiny_id: int) -> None:
    """Adiciona ou atualiza um cliente em TINY_CLIENTE_IDS_JSON no arquivo .env."""
    if not env_path or not env_path.exists():
        return
    content = env_path.read_text(encoding="utf-8")
    match = re.search(r"^(TINY_CLIENTE_IDS_JSON\s*=\s*)(.*)$", content, re.MULTILINE)
    if not match:
        return
    try:
        current: dict[str, int] = json.loads(match.group(2).strip() or "{}")
    except json.JSONDecodeError:
        current = {}
    current[nome] = tiny_id
    new_line = match.group(1) + json.dumps(current, ensure_ascii=False)
    env_path.write_text(content[: match.start()] + new_line + content[match.end() :], encoding="utf-8")


def should_send_accounts_receivable(record: NormalizedRecord, tiny_config: dict[str, Any]) -> bool:
    allowed = tiny_config.get("contas_receber_fp", ["FA"])
    allowed_keys = {normalize_key(str(item)) for item in allowed}
    return normalize_key(record.fp) in allowed_keys


def compact_document_number(config: dict[str, Any], record: NormalizedRecord) -> str:
    """Gera numero de documento com no maximo 9 caracteres (limite da API Tiny).
    Formato: DDMMAA + linha com 3 digitos = 9 chars.
    Exemplo: 150426002 = dia 15, mes 04, ano 26, linha 2.
    """
    year, month, day = record.data.split("-")
    line = str(record.linha_origem % 1000).zfill(3)
    return f"{day}{month}{year[-2:]}{line}"


def build_history(record: NormalizedRecord) -> str:
    return f"Placa {record.placa} | {record.modelo}"[:250]


class TinyClient:
    def __init__(self, tiny_config: dict[str, Any], state_dir: Path) -> None:
        self.config = tiny_config
        self.base_url = tiny_config["base_url"].rstrip("/")
        self.token_url = tiny_config["token_url"]
        self.timeout = int(tiny_config.get("timeout_seconds", 30))
        self.token_file = state_dir / "tiny_tokens.json"
        self._access_token: str | None = None

    def _load_tokens(self) -> dict[str, Any]:
        if self.token_file.exists():
            with self.token_file.open("r", encoding="utf-8") as file:
                return json.load(file)
        return {}

    def _save_tokens(self, tokens: dict[str, Any]) -> None:
        self.token_file.parent.mkdir(parents=True, exist_ok=True)
        with self.token_file.open("w", encoding="utf-8") as file:
            json.dump(tokens, file, ensure_ascii=False, indent=2)

    def access_token(self) -> str:
        if self._access_token:
            return self._access_token

        env_token = os.getenv("TINY_ACCESS_TOKEN")
        if env_token:
            self._access_token = env_token
            return env_token

        tokens = self._load_tokens()
        expires_at = float(tokens.get("expires_at", 0))
        if tokens.get("access_token") and expires_at > time.time() + 60:
            self._access_token = tokens["access_token"]
            return self._access_token

        return self.refresh_access_token(tokens)

    def refresh_access_token(self, tokens: dict[str, Any] | None = None) -> str:
        try:
            import requests
        except ImportError as exc:
            raise TinyApiError("A biblioteca requests e necessaria para enviar ao Tiny.") from exc

        tokens = tokens or self._load_tokens()
        client_id = os.getenv("TINY_CLIENT_ID") or self.config.get("client_id")
        client_secret = os.getenv("TINY_CLIENT_SECRET") or self.config.get("client_secret")
        refresh_token = (
            os.getenv("TINY_REFRESH_TOKEN")
            or tokens.get("refresh_token")
            or self.config.get("refresh_token")
        )

        if not client_id or not client_secret or not refresh_token:
            raise TinyApiError(
                "Sem token do Tiny. Informe TINY_ACCESS_TOKEN, ou TINY_CLIENT_ID/TINY_CLIENT_SECRET/TINY_REFRESH_TOKEN."
            )

        response = requests.post(
            self.token_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "refresh_token",
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
            },
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise TinyApiError(f"Falha ao renovar token Tiny ({response.status_code}): {response.text[:1000]}")

        new_tokens = response.json()
        if "access_token" not in new_tokens:
            raise TinyApiError(f"Resposta de token sem access_token: {new_tokens}")

        new_tokens["expires_at"] = time.time() + int(new_tokens.get("expires_in", 4 * 3600))
        if "refresh_token" not in new_tokens and refresh_token:
            new_tokens["refresh_token"] = refresh_token
        self._save_tokens(new_tokens)
        self._access_token = new_tokens["access_token"]
        return self._access_token

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        retry_auth: bool = True,
    ) -> dict[str, Any]:
        try:
            import requests
        except ImportError as exc:
            raise TinyApiError("A biblioteca requests e necessaria para enviar ao Tiny.") from exc

        url = f"{self.base_url}/{path.lstrip('/')}"
        response = requests.request(
            method,
            url,
            headers={
                "Authorization": f"Bearer {self.access_token()}",
                "Content-Type": "application/json",
            },
            params=params,
            json=json_body,
            timeout=self.timeout,
        )

        if response.status_code == 401 and retry_auth:
            self._access_token = None
            self.refresh_access_token()
            return self.request(method, path, params=params, json_body=json_body, retry_auth=False)

        if response.status_code == 429:
            reset_after = int(response.headers.get("X-RateLimit-Reset", "5"))
            time.sleep(max(reset_after, 1))
            return self.request(method, path, params=params, json_body=json_body, retry_auth=retry_auth)

        if response.status_code >= 400:
            raise TinyApiError(f"Erro Tiny {method} {path} ({response.status_code}): {response.text[:1500]}")

        if not response.text.strip():
            return {}
        return response.json()

    def exchange_authorization_code(self, code: str, redirect_uri: str) -> dict[str, Any]:
        try:
            import requests
        except ImportError as exc:
            raise TinyApiError("A biblioteca requests e necessaria para autenticar no Tiny.") from exc

        client_id = os.getenv("TINY_CLIENT_ID") or self.config.get("client_id")
        client_secret = os.getenv("TINY_CLIENT_SECRET") or self.config.get("client_secret")
        if not client_id or not client_secret:
            raise TinyApiError("Informe TINY_CLIENT_ID e TINY_CLIENT_SECRET para trocar o codigo OAuth.")

        response = requests.post(
            self.token_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "authorization_code",
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": redirect_uri,
                "code": code,
            },
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            message = f"Falha ao trocar codigo OAuth ({response.status_code}): {response.text[:1000]}"
            if "invalid_grant" in response.text:
                message += (
                    "\nO Tiny informou invalid_grant. Gere um code novo com --auth-url, use imediatamente, "
                    "e confira se a redirect_uri e exatamente a mesma do aplicativo."
                )
            raise TinyApiError(message)
        tokens = response.json()
        tokens["expires_at"] = time.time() + int(tokens.get("expires_in", 4 * 3600))
        self._save_tokens(tokens)
        return tokens


class TinyImporter:
    def __init__(self, config: dict[str, Any], state_dir: Path) -> None:
        self.config = config["tiny"]
        self.client = TinyClient(self.config, state_dir)
        self.contact_cache: dict[str, int] = {}
        self.payment_cache: dict[str, int] = {}

    def resolve_contact(self, name: str) -> int:
        key = normalize_key(name)
        if key in self.contact_cache:
            return self.contact_cache[key]

        mapped = lookup_config_id(self.config.get("cliente_ids", {}), name)
        if mapped:
            self.contact_cache[key] = mapped
            return mapped

        # Busca sem filtro de situacao para nao perder contatos com status diferente de "B"
        result = self.client.request("GET", "contatos", params={"nome": name, "limit": 100})
        items = result.get("itens", [])
        contact_id = self._find_exact(items, "nome", name)
        # Tenta tambem pelo nome fantasia
        if not contact_id:
            contact_id = self._find_exact(items, "fantasia", name)
        if contact_id:
            self.contact_cache[key] = contact_id
            return contact_id

        if not self.config.get("auto_create_contacts"):
            raise TinyApiError(
                f"Cliente nao encontrado no Tiny: {name!r}. "
                "Use o modal 'Mapear Clientes' para vincular ao ID correto, "
                "ou habilite auto_create_contacts."
            )

        created = self.client.request(
            "POST",
            "contatos",
            json_body={
                "nome": name,
                "tipoPessoa": self.config.get("default_tipo_pessoa", "J"),
                "situacao": "B",
                "observacoes": "Criado pela importacao automatica da planilha diaria.",
            },
        )
        contact_id = int(created["id"])
        self.contact_cache[key] = contact_id
        return contact_id

    def resolve_payment(self, fp: str) -> int | None:
        if not fp:
            return None
        key = normalize_key(fp)
        if key in self.payment_cache:
            return self.payment_cache[key]

        mapped = lookup_config_id(self.config.get("forma_recebimento_ids", {}), fp)
        if mapped:
            self.payment_cache[key] = mapped
            return mapped

        result = self.client.request("GET", "formas-recebimento", params={"nome": fp, "situacao": 1, "limit": 100})
        payment_id = self._find_exact(result.get("itens", []), "nome", fp)
        if payment_id:
            self.payment_cache[key] = payment_id
            return payment_id

        if self.config.get("require_payment_mapping"):
            raise TinyApiError(f"Forma de recebimento nao encontrada no Tiny para FP={fp!r}.")
        return None

    def build_accounts_receivable_payload(self, record: NormalizedRecord) -> dict[str, Any]:
        client_id = self.resolve_contact(record.cliente)
        # AV: usa a forma real de pagamento definida na frente de caixa (dinheiro/debito/credito/pix)
        # FA: usa o codigo FP do registro (ex: "FA" → "A faturar")
        payment_key = record.av_pagamento if record.av_pagamento else record.fp
        payment_id = self.resolve_payment(payment_key)
        # AV: ja recebido na data do servico; FA: sempre ultimo dia do mes
        due = record.data if record.av_pagamento else last_day_of_month(record.data)

        payload: dict[str, Any] = {
            "data": record.data,
            "dataVencimento": due,
            "dataCompetencia": record.data[:7],
            "valor": money_as_float(record.preco),
            "contato": {"id": client_id},
            "numeroDocumento": compact_document_number(self.config, record),
            "historico": build_history(record),
            "ocorrencia": "U",
        }

        categoria_id = resolve_categoria_id(self.config, record.servico)
        if categoria_id:
            payload["categoria"] = {"id": categoria_id}

        if self.config.get("include_forma_recebimento") and payment_id:
            payload["formaRecebimento"] = payment_id  # Tiny espera int, nao objeto
        return payload

    def create_accounts_receivable(self, record: NormalizedRecord) -> dict[str, Any]:
        payload = self.build_accounts_receivable_payload(record)
        return self.client.request("POST", "contas-receber", json_body=payload)

    @staticmethod
    def _find_exact(items: list[dict[str, Any]], field: str, wanted: str) -> int | None:
        wanted_key = normalize_key(wanted)
        for item in items:
            if normalize_key(str(item.get(field, ""))) == wanted_key and item.get("id"):
                return int(item["id"])
        return None


def write_payload_preview(records: list[NormalizedRecord], output_dir: Path, source: Path, config: dict[str, Any]) -> Path:
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"payload_preview_{source.stem}_{timestamp}.json"
    tiny_config = config["tiny"]
    preview = []
    for record in records:
        enviar_contas_receber = should_send_accounts_receivable(record, tiny_config)
        payload_base: dict[str, Any] = {
            "endpoint": "POST /contas-receber",
            "enviar_contas_receber": enviar_contas_receber,
            "data": record.data,
            "dataVencimento": last_day_of_month(record.data) if enviar_contas_receber else None,
            "dataCompetencia": record.data[:7],
            "valor": money_as_float(record.preco),
            "contato": {
                "nome": record.cliente,
                "id_configurado": lookup_config_id(tiny_config.get("cliente_ids", {}), record.cliente),
            },
            "numeroDocumento": compact_document_number(tiny_config, record),
            "historico": build_history(record),
            "ocorrencia": "U",
        }
        if not enviar_contas_receber:
            payload_base["endpoint"] = None
            payload_base["motivo_nao_envio"] = (
                f"FP {record.fp} nao esta em TINY_CONTAS_RECEBER_FP_JSON. "
                "Linha fica para fechamento/painel, nao para contas a receber."
            )
        categoria_id = resolve_categoria_id(tiny_config, record.servico) if enviar_contas_receber else None
        if categoria_id:
            payload_base["categoria"] = {"id": categoria_id}
        forma_recebimento_id = lookup_config_id(tiny_config.get("forma_recebimento_ids", {}), record.fp)
        if tiny_config.get("include_forma_recebimento") and forma_recebimento_id and enviar_contas_receber:
            payload_base["formaRecebimento"] = forma_recebimento_id

        preview.append(
            {
                "chave_deduplicacao": record.chave_deduplicacao,
                "cliente": record.cliente,
                "servico": record.servico,
                "fp": record.fp,
                "enviar_contas_receber": enviar_contas_receber,
                "aviso": (
                    "No envio real, somente FPs permitidas em TINY_CONTAS_RECEBER_FP_JSON sao enviadas "
                    "ao contas a receber."
                ),
                "payload_base": payload_base,
            }
        )
    with path.open("w", encoding="utf-8") as file:
        json.dump(preview, file, ensure_ascii=False, indent=2)
    return path


def write_run_log(logs_dir: Path, summary: dict[str, Any]) -> Path:
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = logs_dir / f"resumo_importacao_{timestamp}.json"
    with path.open("w", encoding="utf-8") as file:
        json.dump(summary, file, ensure_ascii=False, indent=2)
    return path


def archive_source(source: Path, archive_dir: Path) -> Path:
    target = archive_dir / source.name
    if target.exists():
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        target = archive_dir / f"{source.stem}_{timestamp}{source.suffix}"
    shutil.move(str(source), str(target))
    return target


def process(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    input_dir = project_path(config, "input_dir")
    output_dir = project_path(config, "output_dir")
    archive_dir = project_path(config, "archive_dir")
    state_dir = project_path(config, "state_dir")
    logs_dir = project_path(config, "logs_dir")

    source = args.file if args.file else latest_input(input_dir)
    if not source.is_absolute():
        source = BASE_DIR / source
    if not source.exists():
        raise ImportErrorWithContext(f"Arquivo nao encontrado: {source}")

    records, row_errors = normalize_rows(source, config)
    output_paths = write_outputs(records, output_dir, source)

    summary: dict[str, Any] = {
        "arquivo": str(source),
        "registros_lidos": len(records),
        "registros_enviaveis_contas_receber": sum(
            1 for record in records if should_send_accounts_receivable(record, config["tiny"])
        ),
        "registros_fora_contas_receber": sum(
            1 for record in records if not should_send_accounts_receivable(record, config["tiny"])
        ),
        "erros_linha": row_errors,
        "csv": str(output_paths["csv"]),
        "json": str(output_paths["json"]),
        "dry_run": not args.send,
        "enviados": [],
        "pulados": [],
        "falhas_envio": [],
    }

    if not args.send:
        preview_path = write_payload_preview(records, output_dir, source, config)
        summary["payload_preview"] = str(preview_path)
        summary["log"] = str(write_run_log(logs_dir, summary))
        print_human_summary(summary)
        return 0 if not row_errors else 2

    state_path = state_dir / "imported.json"
    state = load_state(state_path)
    imported = state.setdefault("imported", {})
    importer = TinyImporter(config, state_dir)

    for record in records:
        if not should_send_accounts_receivable(record, config["tiny"]):
            summary["pulados"].append(
                {
                    "chave": record.chave_deduplicacao,
                    "linha": record.linha_origem,
                    "motivo": f"FP {record.fp} fora de TINY_CONTAS_RECEBER_FP_JSON",
                }
            )
            continue
        if record.chave_deduplicacao in imported and not args.force:
            summary["pulados"].append(
                {"chave": record.chave_deduplicacao, "motivo": "ja importado", "tiny": imported[record.chave_deduplicacao]}
            )
            continue
        try:
            response = importer.create_accounts_receivable(record)
            imported[record.chave_deduplicacao] = {
                "arquivo": record.origem_arquivo,
                "linha": record.linha_origem,
                "enviado_em": dt.datetime.now().isoformat(timespec="seconds"),
                "resposta": response,
            }
            save_state(state_path, state)
            summary["enviados"].append({"chave": record.chave_deduplicacao, "resposta": response})
        except Exception as exc:
            if _is_doc_already_registered(exc):
                imported[record.chave_deduplicacao] = {
                    "arquivo": record.origem_arquivo,
                    "linha": record.linha_origem,
                    "enviado_em": dt.datetime.now().isoformat(timespec="seconds"),
                    "motivo": "ja existia no Tiny (numeroDocumento duplicado)",
                }
                save_state(state_path, state)
                summary["pulados"].append(
                    {"chave": record.chave_deduplicacao, "motivo": "ja existia no Tiny (numeroDocumento duplicado)"}
                )
            else:
                summary["falhas_envio"].append(
                    {"chave": record.chave_deduplicacao, "linha": record.linha_origem, "erro": str(exc)}
                )

    if args.archive and not summary["falhas_envio"]:
        summary["arquivado_em"] = str(archive_source(source, archive_dir))

    summary["log"] = str(write_run_log(logs_dir, summary))
    print_human_summary(summary)
    return 1 if summary["falhas_envio"] or row_errors else 0


def print_human_summary(summary: dict[str, Any]) -> None:
    print(f"Arquivo: {summary['arquivo']}")
    print(f"Registros padronizados: {summary['registros_lidos']}")
    print(f"Enviaveis ao contas a receber: {summary.get('registros_enviaveis_contas_receber', 0)}")
    print(f"Fora do contas a receber: {summary.get('registros_fora_contas_receber', 0)}")
    print(f"CSV: {summary['csv']}")
    print(f"JSON: {summary['json']}")
    if summary.get("payload_preview"):
        print(f"Preview Tiny: {summary['payload_preview']}")
    if summary["erros_linha"]:
        print("Erros de linha:")
        for error in summary["erros_linha"]:
            print(f"  - {error}")
    if not summary["dry_run"]:
        print(f"Enviados: {len(summary['enviados'])}")
        print(f"Pulados: {len(summary['pulados'])}")
        print(f"Falhas: {len(summary['falhas_envio'])}")
    print(f"Log: {summary['log']}")


def print_table(items: list[dict[str, Any]], columns: list[tuple[str, str]]) -> None:
    if not items:
        print("Nenhum registro encontrado.")
        return
    widths = []
    for header, key in columns:
        max_value = max(len(str(item.get(key, ""))) for item in items)
        widths.append(max(len(header), max_value))
    print("  ".join(header.ljust(width) for (header, _key), width in zip(columns, widths)))
    print("  ".join("-" * width for width in widths))
    for item in items:
        print("  ".join(str(item.get(key, "")).ljust(width) for (_header, key), width in zip(columns, widths)))


def list_tiny_resource(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    state_dir = project_path(config, "state_dir")
    client = TinyClient(config["tiny"], state_dir)

    if args.list_clientes is not None:
        params: dict[str, Any] = {"situacao": "B", "limit": 100}
        if args.list_clientes:
            params["nome"] = args.list_clientes
        response = client.request("GET", "contatos", params=params)
        items = [
            {
                "id": item.get("id", ""),
                "nome": item.get("nome", ""),
                "fantasia": item.get("fantasia", ""),
                "cpfCnpj": item.get("cpfCnpj", ""),
            }
            for item in response.get("itens", [])
        ]
        print_table(items, [("id", "id"), ("nome", "nome"), ("fantasia", "fantasia"), ("cpfCnpj", "cpfCnpj")])
        return 0

    if args.list_formas_recebimento:
        response = client.request("GET", "formas-recebimento", params={"situacao": 1, "limit": 100})
        items = [
            {"id": item.get("id", ""), "nome": item.get("nome", ""), "situacao": item.get("situacao", "")}
            for item in response.get("itens", [])
        ]
        print_table(items, [("id", "id"), ("nome", "nome"), ("situacao", "situacao")])
        return 0

    if args.list_categorias:
        response = client.request("GET", "categorias-receita-despesa", params={"limit": 100})
        raw_items = response.get("itens", [])
        items = [
            {
                "id": item.get("id", ""),
                "descricao": item.get("descricao") or item.get("nome", ""),
                "grupo": item.get("grupo", ""),
            }
            for item in raw_items
        ]
        print_table(items, [("id", "id"), ("descricao", "descricao"), ("grupo", "grupo")])
        return 0

    return 0


def check_env(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    tiny = config["tiny"]
    state_dir = project_path(config, "state_dir")
    token_file = state_dir / "tiny_tokens.json"

    statuses: list[tuple[str, str, str]] = []

    def add(name: str, ok: bool, detail: str = "") -> None:
        statuses.append(("OK" if ok else "FALTA", name, detail))

    has_client_id = bool(tiny.get("client_id"))
    has_client_secret = bool(tiny.get("client_secret"))
    has_redirect_uri = bool(tiny.get("redirect_uri"))
    env_access_token = os.getenv("TINY_ACCESS_TOKEN", "")
    has_access_token = bool(env_access_token)
    has_legacy_access_token = env_access_token.startswith("tiny-api-")
    has_refresh_token = bool(tiny.get("refresh_token"))
    has_saved_token = token_file.exists()

    add("TINY_CLIENT_ID", has_client_id, "preenchido" if has_client_id else "vazio")
    add("TINY_CLIENT_SECRET", has_client_secret, "preenchido" if has_client_secret else "vazio")
    add("TINY_REDIRECT_URI", has_redirect_uri, tiny.get("redirect_uri") or "vazio")
    add(
        "Token OAuth",
        has_access_token or has_refresh_token or has_saved_token,
        "possivel token legado tiny-api-" if has_legacy_access_token else (
            "ok" if has_access_token or has_refresh_token or has_saved_token else "sem access/refresh token salvo"
        ),
    )
    add("TINY_VENCIMENTO_DIAS", isinstance(tiny.get("vencimento_dias"), int), str(tiny.get("vencimento_dias")))
    add("TINY_VENCIMENTO_TIPO", bool(tiny.get("vencimento_tipo")), str(tiny.get("vencimento_tipo") or "vazio"))
    add(
        "TINY_CONTAS_RECEBER_FP_JSON",
        isinstance(tiny.get("contas_receber_fp"), list),
        ",".join(str(item) for item in tiny.get("contas_receber_fp", [])),
    )
    add("TINY_CLIENTE_IDS_JSON", isinstance(tiny.get("cliente_ids"), dict), f"{len(tiny.get('cliente_ids', {}))} cliente(s)")
    add(
        "TINY_FORMA_RECEBIMENTO_IDS_JSON",
        isinstance(tiny.get("forma_recebimento_ids"), dict),
        f"{len(tiny.get('forma_recebimento_ids', {}))} forma(s)",
    )
    add(
        "TINY_SERVICO_ALIASES_JSON",
        isinstance(tiny.get("aliases", {}).get("servico"), dict),
        f"{len(tiny.get('aliases', {}).get('servico', {}))} alias(es)",
    )
    add(
        "TINY_FP_ALIASES_JSON",
        isinstance(tiny.get("aliases", {}).get("fp"), dict),
        f"{len(tiny.get('aliases', {}).get('fp', {}))} alias(es)",
    )
    add(
        "TINY_CLIENTE_ALIASES_JSON",
        isinstance(tiny.get("aliases", {}).get("cliente"), dict),
        f"{len(tiny.get('aliases', {}).get('cliente', {}))} alias(es)",
    )
    categoria_id = tiny.get("categoria_id")
    add("TINY_CATEGORIA_ID", True, "vazio (opcional)" if not categoria_id else "preenchido")

    for status, name, detail in statuses:
        print(f"{status} - {name}: {detail}")

    ready_to_authorize = has_client_id and has_client_secret and has_redirect_uri
    ready_to_call_api = has_access_token or has_refresh_token or has_saved_token
    if not ready_to_authorize:
        print("\nPara autorizar o app, preencha TINY_CLIENT_ID, TINY_CLIENT_SECRET e TINY_REDIRECT_URI.")
    if not ready_to_call_api:
        print("Para consultar/enviar no Tiny, gere o token com --exchange-code ou preencha TINY_ACCESS_TOKEN/TINY_REFRESH_TOKEN.")
    if has_legacy_access_token:
        print("Aviso: TINY_ACCESS_TOKEN parece token antigo iniciado por tiny-api-. A API v3 espera token OAuth Bearer.")
    return 0 if ready_to_authorize and ready_to_call_api else 2


def print_auth_url(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    tiny = config["tiny"]
    client_id = tiny.get("client_id") or os.getenv("TINY_CLIENT_ID")
    redirect_uri = args.redirect_uri or tiny.get("redirect_uri") or os.getenv("TINY_REDIRECT_URI")
    if not client_id:
        raise TinyApiError("Preencha TINY_CLIENT_ID antes de gerar a URL de autorizacao.")
    if not redirect_uri:
        raise TinyApiError("Preencha TINY_REDIRECT_URI ou passe --redirect-uri antes de gerar a URL de autorizacao.")

    print(build_auth_url(tiny, client_id, redirect_uri))
    print("\nAbra essa URL, autorize o app e copie apenas o valor depois de code=.")
    return 0


def build_auth_url(tiny: dict[str, Any], client_id: str, redirect_uri: str, state: str | None = None) -> str:
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": tiny.get("oauth_scope", "openid"),
        "response_type": "code",
    }
    if state:
        params["state"] = state
    query = urllib.parse.urlencode(params)
    return f"{tiny.get('auth_url').rstrip('/')}?{query}"


def extract_oauth_code(value: str) -> str:
    value = value.strip().strip('"').strip("'")
    if not value:
        return value
    if value.startswith("http://") or value.startswith("https://"):
        parsed = urllib.parse.urlparse(value)
        query = urllib.parse.parse_qs(parsed.query)
        if query.get("code"):
            return query["code"][0]
    if value.startswith("code="):
        return value.split("=", 1)[1].split("&", 1)[0]
    if "code=" in value:
        return value.split("code=", 1)[1].split("&", 1)[0]
    return value


def run_oauth_local(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    tiny = config["tiny"]
    state_dir = project_path(config, "state_dir")
    client_id = tiny.get("client_id") or os.getenv("TINY_CLIENT_ID")
    redirect_uri = args.redirect_uri or tiny.get("redirect_uri") or os.getenv("TINY_REDIRECT_URI")
    if not client_id:
        raise TinyApiError("Preencha TINY_CLIENT_ID antes de iniciar o login local.")
    if not redirect_uri:
        raise TinyApiError("Preencha TINY_REDIRECT_URI antes de iniciar o login local.")

    parsed_redirect = urllib.parse.urlparse(redirect_uri)
    if parsed_redirect.scheme != "http" or parsed_redirect.hostname not in {"localhost", "127.0.0.1"}:
        raise TinyApiError("O login local exige TINY_REDIRECT_URI como http://localhost:PORTA/callback.")
    if not parsed_redirect.port:
        raise TinyApiError("Informe uma porta na TINY_REDIRECT_URI, por exemplo http://localhost:8080/callback.")

    expected_state = secrets.token_urlsafe(16)
    auth_url = build_auth_url(tiny, client_id, redirect_uri, state=expected_state)
    captured: dict[str, str] = {}
    expected_path = parsed_redirect.path or "/callback"

    class CallbackHandler(http.server.BaseHTTPRequestHandler):
        def log_message(self, _format: str, *_args: Any) -> None:
            return

        def do_GET(self) -> None:
            request = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(request.query)
            if request.path != expected_path:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"Callback path invalido.")
                return
            received_state = params.get("state", [""])[0]
            if received_state != expected_state:
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(
                    b"<html><body><h1>Callback antigo ignorado</h1>"
                    b"<p>Volte ao terminal e abra a URL nova impressa por ele.</p></body></html>"
                )
                return
            if params.get("error"):
                captured["error"] = params["error"][0]
                captured["error_description"] = params.get("error_description", [""])[0]
            elif params.get("code"):
                captured["code"] = params["code"][0]
            else:
                captured["error"] = "missing_code"
                captured["error_description"] = "Callback recebido sem code."

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                b"<html><body><h1>Autorizacao recebida</h1>"
                b"<p>Voce ja pode voltar para o terminal.</p></body></html>"
            )

    server = http.server.HTTPServer((parsed_redirect.hostname, parsed_redirect.port), CallbackHandler)
    print(f"Aguardando callback em {redirect_uri}")
    print("Abra esta URL no navegador e autorize o aplicativo:")
    print(auth_url)
    if args.open_browser:
        webbrowser.open(auth_url)
    deadline = time.time() + args.oauth_timeout
    while not captured and time.time() < deadline:
        server.timeout = max(1, min(5, int(deadline - time.time())))
        server.handle_request()
    server.server_close()

    if captured.get("error"):
        detail = captured.get("error_description", "")
        raise TinyApiError(f"Autorizacao recusada ou invalida: {captured['error']} {detail}".strip())
    if not captured.get("code"):
        raise TinyApiError("Nao recebi o callback OAuth. Gere novamente ou confira a redirect URI no aplicativo.")

    tokens = TinyClient(tiny, state_dir).exchange_authorization_code(captured["code"], redirect_uri)
    print(f"Tokens salvos em {state_dir / 'tiny_tokens.json'}")
    print(f"Access token expira em {tokens.get('expires_in', 'desconhecido')} segundos.")
    return 0


def run_server(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    port = args.port or config.get("server", {}).get("port", 8081)
    host = config.get("server", {}).get("host", "localhost")
    state_dir = project_path(config, "state_dir")
    ui_dir = BASE_DIR / "frente_caixa"
    env_file_path: Path = args.env_file if args.env_file else (BASE_DIR / ".env")

    if not ui_dir.exists():
        raise ImportErrorWithContext(f"Pasta da interface nao encontrada: {ui_dir}")

    class IntegrationHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(ui_dir), **kwargs)

        def log_message(self, format, *args):
            return

        def do_GET(self):
            if self.path == "/api/info":
                unidade = os.environ.get("TINY_UNIDADE_NOME", "")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"unidade": unidade}).encode())
            else:
                super().do_GET()

        def do_POST(self):
            if self.path == "/api/send":
                self._handle_api_send()
            elif self.path == "/api/preview":
                self._handle_api_preview()
            elif self.path == "/api/suggest-clients":
                self._handle_api_suggest_clients()
            elif self.path == "/api/map-client":
                self._handle_api_map_client()
            else:
                self.send_error(404, "Endpoint nao encontrado")

        def _handle_api_send(self):
            try:
                content_length = int(self.headers["Content-Length"])
                body = self.rfile.read(content_length)
                request_data = json.loads(body)
                
                records_data = request_data.get("records", [])
                source_name = request_data.get("source", "manual_ui")

                normalized_records = []
                for r in records_data:
                    normalized_records.append(NormalizedRecord(
                        data=r["data"],
                        modelo=r["modelo"],
                        placa=r["placa"],
                        cliente=r["cliente"],
                        servico=r["servico"],
                        fp=r["fp"],
                        preco=str(r["preco"]),
                        origem_arquivo=r.get("origemArquivo", source_name),
                        linha_origem=r.get("linhaOrigem", 0),
                        chave_deduplicacao=r.get("id", "missing_key"),
                        av_pagamento=r.get("avPagamento", "")
                    ))

                for r in normalized_records:
                    if r.chave_deduplicacao == "missing_key" or "-" in r.chave_deduplicacao:
                        r.chave_deduplicacao = record_key(asdict(r))

                state_path = state_dir / "imported.json"
                state = load_state(state_path)
                imported = state.setdefault("imported", {})
                importer = TinyImporter(config, state_dir)
                
                results = {"enviados": [], "pulados": [], "falhas": []}

                for record in normalized_records:
                    if record.chave_deduplicacao in imported:
                        results["pulados"].append({"chave": record.chave_deduplicacao, "motivo": "ja importado"})
                        continue
                    
                    try:
                        response = importer.create_accounts_receivable(record)
                        imported[record.chave_deduplicacao] = {
                            "arquivo": record.origem_arquivo,
                            "linha": record.linha_origem,
                            "enviado_em": dt.datetime.now().isoformat(timespec="seconds"),
                            "resposta": response,
                        }
                        save_state(state_path, state)
                        results["enviados"].append({"chave": record.chave_deduplicacao, "cliente": record.cliente})
                    except Exception as exc:
                        if _is_doc_already_registered(exc):
                            imported[record.chave_deduplicacao] = {
                                "arquivo": record.origem_arquivo,
                                "linha": record.linha_origem,
                                "enviado_em": dt.datetime.now().isoformat(timespec="seconds"),
                                "motivo": "ja existia no Tiny (numeroDocumento duplicado)",
                            }
                            save_state(state_path, state)
                            results["pulados"].append({"chave": record.chave_deduplicacao, "cliente": record.cliente, "motivo": "ja existia no Tiny (numeroDocumento duplicado)"})
                        else:
                            results["falhas"].append({"chave": record.chave_deduplicacao, "cliente": record.cliente, "erro": str(exc)})

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "success": True, 
                    "summary": results,
                    "message": f"Processamento concluido. Enviados: {len(results['enviados'])}"
                }).encode())

            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"success": False, "error": str(e)}).encode())

        def _handle_api_preview(self):
            """Retorna o payload que seria enviado ao Tiny, sem fazer nenhuma chamada de API."""
            _forma_names: dict[int, str] = {
                556498207: "Dinheiro",
                556498209: "Cartao de credito",
                556498211: "Cartao de debito",
                556498213: "Boleto",
                556498217: "Deposito",
                802165201: "A faturar",
                802165265: "Cortesia",
                803887338: "Retorno",
            }
            try:
                content_length = int(self.headers["Content-Length"])
                body = self.rfile.read(content_length)
                request_data = json.loads(body)
                records_data = request_data.get("records", [])
                source_name = request_data.get("source", "manual_ui")

                state_path = state_dir / "imported.json"
                state_data = load_state(state_path)
                imported = state_data.get("imported", {})

                tiny_config = config["tiny"]
                forma_ids = tiny_config.get("forma_recebimento_ids", {})

                previews = []
                for r in records_data:
                    chave = r.get("id", "?")
                    av_pag = r.get("avPagamento", "")
                    fp = r.get("fp", "")
                    payment_key = av_pag if av_pag else fp
                    payment_id = lookup_config_id(forma_ids, payment_key)

                    rec = NormalizedRecord(
                        data=r["data"],
                        modelo=r.get("modelo", ""),
                        placa=r.get("placa", ""),
                        cliente=r.get("cliente", ""),
                        servico=r.get("servico", ""),
                        fp=fp,
                        preco=str(r.get("preco", "0")),
                        origem_arquivo=r.get("origemArquivo", source_name),
                        linha_origem=r.get("linhaOrigem", 0),
                        chave_deduplicacao=chave,
                        av_pagamento=av_pag,
                    )

                    due = rec.data if av_pag else last_day_of_month(rec.data)
                    num_doc = compact_document_number(tiny_config, rec)
                    forma_display = (
                        f"{_forma_names.get(payment_id, str(payment_id))} (ID {payment_id})"
                        if payment_id else "nao mapeado"
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
                    if payment_id and tiny_config.get("include_forma_recebimento"):
                        payload["formaRecebimento"] = payment_id  # Tiny espera int
                    categoria_id = resolve_categoria_id(tiny_config, rec.servico)
                    if categoria_id:
                        payload["categoria"] = {"id": categoria_id}

                    previews.append({
                        "chave": chave,
                        "cliente": rec.cliente,
                        "fp": fp,
                        "avPagamento": av_pag,
                        "valor": money_as_float(rec.preco),
                        "dataVencimento": due,
                        "formaRecebimento": forma_display,
                        "numeroDocumento": num_doc,
                        "jaEnviado": chave in imported,
                        "payload": payload,
                    })

                novos = sum(1 for p in previews if not p["jaEnviado"])
                duplicatas = sum(1 for p in previews if p["jaEnviado"])

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "success": True,
                    "previews": previews,
                    "resumo": {"novos": novos, "duplicatas": duplicatas, "total": len(previews)},
                }).encode())

            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"success": False, "error": str(e)}).encode())

        def _handle_api_suggest_clients(self):
            """Busca candidatos no Tiny para um nome de cliente da planilha."""
            try:
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                nome = clean_text(body.get("nome", ""))
                if not nome:
                    raise ValueError("nome obrigatorio")

                importer = TinyImporter(config, state_dir)
                result = importer.client.request("GET", "contatos", params={"nome": nome, "limit": 20})
                candidates = []
                for item in result.get("itens", []):
                    item_nome = item.get("nome", "")
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

                # Busca adicional sem filtro de nome se poucos resultados
                if len(candidates) < 3:
                    result2 = importer.client.request("GET", "contatos", params={"limit": 100})
                    seen = {c["id"] for c in candidates}
                    for item in result2.get("itens", []):
                        if item.get("id") in seen:
                            continue
                        item_nome = item.get("nome", "")
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
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"success": True, "candidates": candidates[:6]}).encode())
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"success": False, "error": str(e)}).encode())

        def _handle_api_map_client(self):
            """Confirma o vinculo cliente planilha → ID Tiny e salva no .env."""
            try:
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                cliente_nome = clean_text(body.get("clienteNome", ""))
                tiny_id = body.get("tinyId")
                if not cliente_nome or not tiny_id:
                    raise ValueError("clienteNome e tinyId obrigatorios")

                config["tiny"].setdefault("cliente_ids", {})[cliente_nome] = int(tiny_id)
                _update_env_cliente_id(env_file_path, cliente_nome, int(tiny_id))

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"success": True, "saved": True}).encode())
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"success": False, "error": str(e)}).encode())

    server = http.server.HTTPServer((host, port), IntegrationHandler)
    print(f"Interface de Conferencia rodando em: http://{host}:{port}")
    print("Pressione Ctrl+C para encerrar.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServidor encerrado.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Padroniza a planilha diaria e, opcionalmente, envia contas a receber ao Tiny/Olist."
    )
    parser.add_argument("--file", type=Path, help="Arquivo .xls/.html para processar. Se omitido, usa o mais novo em entrada/.")
    parser.add_argument("--config", type=Path, help="Caminho do config.json.")
    parser.add_argument("--env-file", type=Path, help="Caminho do arquivo .env. Se omitido, usa .env na pasta do script.")
    parser.add_argument("--send", action="store_true", help="Envia de verdade para o Tiny. Sem isso, roda em dry-run.")
    parser.add_argument("--force", action="store_true", help="Reenvia mesmo se a chave ja estiver em state/imported.json.")
    parser.add_argument("--archive", action="store_true", help="Move o arquivo para processados/ apos envio sem falhas.")
    parser.add_argument("--exchange-code", help="Troca um codigo OAuth do Tiny por tokens e salva em state/tiny_tokens.json.")
    parser.add_argument("--redirect-uri", help="Redirect URI usado na troca do codigo OAuth.")
    parser.add_argument(
        "--list-clientes",
        nargs="?",
        const="",
        metavar="NOME",
        help="Lista clientes/contatos do Tiny. Opcionalmente filtre por nome.",
    )
    parser.add_argument("--list-formas-recebimento", action="store_true", help="Lista formas de recebimento do Tiny.")
    parser.add_argument("--list-categorias", action="store_true", help="Lista categorias financeiras do Tiny.")
    parser.add_argument("--check-env", action="store_true", help="Valida o .env sem mostrar segredos.")
    parser.add_argument("--auth-url", action="store_true", help="Gera a URL OAuth de autorizacao usando o .env.")
    parser.add_argument("--oauth-local", action="store_true", help="Inicia callback local e troca o code OAuth automaticamente.")
    parser.add_argument("--oauth-timeout", type=int, default=180, help="Tempo maximo em segundos aguardando callback local.")
    parser.add_argument("--open-browser", action="store_true", help="Tenta abrir a URL OAuth no navegador ao usar --oauth-local.")
    parser.add_argument("--serve-ui", action="store_true", help="Inicia o servidor da interface web de conferencia.")
    parser.add_argument("--port", type=int, help="Porta para o servidor da interface (padrao: 8081).")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        load_env_file(args.env_file)
        if args.exchange_code:
            config = load_config(args.config)
            state_dir = project_path(config, "state_dir")
            redirect_uri = args.redirect_uri or os.getenv("TINY_REDIRECT_URI") or config["tiny"].get("redirect_uri")
            if not redirect_uri:
                raise TinyApiError("Informe --redirect-uri ou TINY_REDIRECT_URI para trocar o codigo OAuth.")
            oauth_code = extract_oauth_code(args.exchange_code)
            tokens = TinyClient(config["tiny"], state_dir).exchange_authorization_code(oauth_code, redirect_uri)
            print(f"Tokens salvos em {state_dir / 'tiny_tokens.json'}")
            print(f"Access token expira em {tokens.get('expires_in', 'desconhecido')} segundos.")
            return 0
        if args.auth_url:
            return print_auth_url(args)
        if args.oauth_local:
            return run_oauth_local(args)
        if args.check_env:
            return check_env(args)
        if args.list_clientes is not None or args.list_formas_recebimento or args.list_categorias:
            return list_tiny_resource(args)
        if args.serve_ui:
            return run_server(args)
        return process(args)
    except (ImportErrorWithContext, TinyApiError, ValueError) as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
