"""
Cliente Omie API + Importer pra envio de contas a receber.

Paralelo ao tiny_import.py. Diferenças principais:
- Auth: app_key + app_secret no body (não OAuth como Tiny)
- Estilo: JSON-RPC (sempre POST com {call, app_key, app_secret, param})
- Categoria: string hierárquica ("1.01.01") em vez de int
- Conta corrente: obrigatória em cada lançamento (id_conta_corrente int)

Reusa NormalizedRecord, build_history, last_day_of_month, normalize_key,
record_key do tiny_import — esses são utilitários de domínio, não Tiny-specific.

Uso típico:
    config = {"omie": {"app_key": "...", "app_secret": "...",
                       "id_conta_corrente": 123, "categoria_ids": {...}}}
    importer = OmieImporter(config, state_dir)
    result = importer.create_accounts_receivable(rec)
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

import requests

from tiny_import import (
    NormalizedRecord,
    build_history,
    last_day_of_month,
    normalize_key,
    is_av_paid,
    money_as_float,
)


_log = logging.getLogger(__name__)

OMIE_BASE = "https://app.omie.com.br/api/v1"


class OmieApiError(Exception):
    """Erro retornado pela API Omie (faultstring) ou rede.

    .code = faultcode (string), .descricao = faultstring (string)
    """
    def __init__(self, code: str, descricao: str, raw: Any = None):
        super().__init__(f"Omie [{code}]: {descricao}")
        self.code = code
        self.descricao = descricao
        self.raw = raw


def _is_omie_redundant_error(exc: Exception) -> bool:
    """Detecta erro 'consumo redundante' do Omie (anti-flood).

    Omie bloqueia chamadas identicas em curta janela (~1 minuto). Mensagem
    tipica: 'Consumo redundante detectado. Aguarde 57 segundos para tentar
    novamente (REDUNDANT)'.

    Quando isso acontece, NAO sabemos se a chamada anterior foi sucesso ou
    falha — Omie ja a tem. Caller deve marcar como 'pulado' e nao retentar
    pra evitar travar mais ainda.
    """
    msg = str(exc).lower()
    return "redundant" in msg or "consumo redundante" in msg


class OmieClient:
    """Cliente JSON-RPC pra Omie. Auth via app_key + app_secret no body.

    Sem OAuth, sem refresh token — credenciais são estáticas do app criado no
    portal Omie. Não persiste nada localmente; toda chamada vai com auth fresca.
    """
    def __init__(self, app_key: str, app_secret: str, timeout: int = 30) -> None:
        self.app_key = app_key
        self.app_secret = app_secret
        self.timeout = timeout
        self.session = requests.Session()

    def request(self, endpoint: str, call: str, param: dict | list[dict]) -> dict:
        """Faz POST JSON-RPC pro endpoint Omie.

        endpoint: parte após /api/v1/. Ex: 'financas/contareceber'.
        call: nome do método. Ex: 'IncluirContaReceber'.
        param: dict (vira [param]) ou lista de dicts.

        Retorna o body parseado. Levanta OmieApiError se rede/HTTP/Omie falharem.
        """
        url = f"{OMIE_BASE}/{endpoint.strip('/')}/"
        payload = {
            "call":       call,
            "app_key":    self.app_key,
            "app_secret": self.app_secret,
            "param":      [param] if isinstance(param, dict) else list(param),
        }
        try:
            resp = self.session.post(url, json=payload, timeout=self.timeout)
        except requests.RequestException as exc:
            raise OmieApiError("REDE", f"falha de rede: {exc}") from exc
        try:
            data = resp.json()
        except ValueError as exc:
            raise OmieApiError(
                f"HTTP{resp.status_code}",
                f"resposta nao-JSON: {resp.text[:200]}"
            ) from exc
        # Omie indica erro via faultstring no body (mesmo com HTTP 500)
        if isinstance(data, dict) and "faultstring" in data:
            raise OmieApiError(
                str(data.get("faultcode", "?")),
                str(data.get("faultstring", "erro desconhecido")),
                raw=data,
            )
        return data


class OmieImporter:
    """Importer de contas a receber pro Omie. Espelha API publica do TinyImporter.

    Métodos principais:
    - testar_conexao()           -> dict {ok, categorias_total} ou {ok: False, error}
    - listar_categorias()        -> list[dict] paginado
    - listar_contas_correntes()  -> list[dict]
    - resolve_contact(nome, cpf) -> int (codigo_cliente_fornecedor)
    - resolve_categoria(servico) -> str | None (codigo "1.01.01")
    - create_accounts_receivable(rec) -> dict (com codigo_lancamento_omie)
    """
    def __init__(self, config: dict[str, Any], state_dir: Path):
        self.config = config
        self.state_dir = Path(state_dir)
        omie_cfg = (config or {}).get("omie") or {}
        self.client = OmieClient(
            app_key=str(omie_cfg.get("app_key", "")),
            app_secret=str(omie_cfg.get("app_secret", "")),
            timeout=int(omie_cfg.get("timeout_seconds", 30)),
        )
        self.id_conta_corrente = int(omie_cfg.get("id_conta_corrente", 0) or 0)
        self.categoria_ids: dict[str, str] = {
            str(k).upper().strip(): str(v).strip()
            for k, v in (omie_cfg.get("categoria_ids") or {}).items()
            if str(k).strip() and str(v).strip()
        }
        self._contact_cache: dict[str, int] = {}

    # ── conexao ───────────────────────────────────────────────────────────────

    def testar_conexao(self) -> dict:
        """Valida credenciais via ListarCategorias (chamada barata).

        Retorna {ok: True, categorias_total: n} ou {ok: False, code, error}.
        Não lança — uso pretendido eh exibir status na UI de config.
        """
        try:
            r = self.client.request(
                "geral/categorias", "ListarCategorias",
                {"pagina": 1, "registros_por_pagina": 1, "filtrar_apenas_ativo": "S"},
            )
            return {"ok": True, "categorias_total": int(r.get("total_de_registros", 0) or 0)}
        except OmieApiError as exc:
            return {"ok": False, "code": exc.code, "error": exc.descricao}

    # ── listagens (pra UI de config) ──────────────────────────────────────────

    def listar_categorias(self, max_pages: int = 20) -> list[dict]:
        """Pagina ListarCategorias. Retorna [{codigo, descricao, conta_inativa, ...}]."""
        out: list[dict] = []
        page = 1
        while page <= max_pages:
            r = self.client.request(
                "geral/categorias", "ListarCategorias",
                {"pagina": page, "registros_por_pagina": 100, "filtrar_apenas_ativo": "S"},
            )
            out.extend(r.get("categoria_cadastro") or [])
            if page >= int(r.get("total_de_paginas", 1) or 1):
                break
            page += 1
        return out

    def listar_contas_correntes(self, max_pages: int = 5) -> list[dict]:
        """Lista contas correntes da empresa. Pra escolher id_conta_corrente."""
        out: list[dict] = []
        page = 1
        while page <= max_pages:
            r = self.client.request(
                "geral/contacorrente", "ListarContasCorrentes",
                {"pagina": page, "registros_por_pagina": 100, "apenas_importado_api": "N"},
            )
            out.extend(r.get("ListarContasCorrentes") or r.get("conta_corrente_cadastro") or [])
            if page >= int(r.get("total_de_paginas", 1) or 1):
                break
            page += 1
        return out

    # ── resolve (cliente / categoria) ─────────────────────────────────────────

    def resolve_contact(self, cliente_nome: str, cpf: str = "") -> int:
        """Busca cliente por CNPJ/CPF (se houver) ou por razão social. Cria se nao existe.

        Retorna codigo_cliente_omie (int). Levanta OmieApiError se falhar.
        Cache em memória por chave normalizada (vida do importer).
        """
        nome = (cliente_nome or "").strip()
        if not nome:
            raise OmieApiError("CLIENTE", "nome do cliente vazio")
        cache_key = normalize_key(f"{nome}|{cpf}")
        if cache_key in self._contact_cache:
            return self._contact_cache[cache_key]

        # 1) Tenta achar por CPF/CNPJ se foi informado
        cpf_clean = "".join(c for c in (cpf or "") if c.isdigit())
        if cpf_clean and len(cpf_clean) in (11, 14):
            try:
                r = self.client.request(
                    "geral/clientes", "ConsultarCliente",
                    {"cnpj_cpf": cpf_clean},
                )
                cid = int(r.get("codigo_cliente_omie", 0) or 0)
                if cid:
                    self._contact_cache[cache_key] = cid
                    return cid
            except OmieApiError as exc:
                # codigo 5113 = cliente nao encontrado — segue pra busca por nome
                if exc.code not in ("SOAP-ENV:Client-5113", "5113"):
                    _log.warning("[omie] busca por CPF falhou: %s", exc)

        # 2) Busca por razão social
        try:
            r = self.client.request(
                "geral/clientes", "ListarClientes",
                {"pagina": 1, "registros_por_pagina": 50, "apenas_importado_api": "N",
                 "clientesFiltro": {"razao_social": nome}},
            )
            for c in (r.get("clientes_cadastro") or []):
                if normalize_key(c.get("razao_social", "")) == normalize_key(nome):
                    cid = int(c.get("codigo_cliente_omie", 0) or 0)
                    if cid:
                        self._contact_cache[cache_key] = cid
                        return cid
        except OmieApiError as exc:
            _log.warning("[omie] listar por nome falhou: %s", exc)

        # 3) Cria
        return self._criar_contato(nome, cpf_clean, cache_key)

    def _criar_contato(self, nome: str, cpf_clean: str, cache_key: str) -> int:
        """Cria contato no Omie via IncluirCliente. Retorna codigo_cliente_omie."""
        param = {
            "razao_social": nome[:60],
            "nome_fantasia": nome[:60],
            "cnpj_cpf": cpf_clean,
            "tags": [{"tag": "Astro Vistorias"}],
        }
        if cpf_clean:
            param["pessoa_fisica"] = "S" if len(cpf_clean) == 11 else "N"
        try:
            r = self.client.request("geral/clientes", "IncluirCliente", param)
        except OmieApiError as exc:
            # Se ja cadastrado pelo CNPJ (5113), tenta achar de novo
            if "ja consta cadastrado" in (exc.descricao or "").lower():
                if cpf_clean:
                    r2 = self.client.request(
                        "geral/clientes", "ConsultarCliente",
                        {"cnpj_cpf": cpf_clean},
                    )
                    cid = int(r2.get("codigo_cliente_omie", 0) or 0)
                    if cid:
                        self._contact_cache[cache_key] = cid
                        return cid
            raise
        cid = int(r.get("codigo_cliente_omie", 0) or 0)
        if not cid:
            raise OmieApiError("CLIENTE", f"resposta sem codigo_cliente_omie: {r}")
        self._contact_cache[cache_key] = cid
        return cid

    def resolve_categoria(self, servico: str) -> str | None:
        """Busca codigo_categoria no config local (categoria_ids).

        Match por substring (igual TinyImporter.resolve_categoria_id) — pega a
        primeira categoria cuja chave normalizada bate com o serviço.
        """
        if not servico or not self.categoria_ids:
            return None
        servico_key = normalize_key(servico)
        for nome, cod in self.categoria_ids.items():
            nome_key = normalize_key(nome)
            if nome_key in servico_key or servico_key in nome_key:
                return cod
        return None

    # ── enviar ────────────────────────────────────────────────────────────────

    def create_accounts_receivable(self, record: NormalizedRecord) -> dict:
        """Cria conta a receber no Omie. Retorna dict com codigo_lancamento_omie.

        Levanta OmieApiError se rede/Omie falharem (api_send trata).
        """
        if not self.id_conta_corrente:
            raise OmieApiError(
                "CONFIG",
                "id_conta_corrente nao configurada — ver /master/unidades/<slug>/erp-config",
            )

        cliente_id = self.resolve_contact(record.cliente, record.cpf)
        categoria = self.resolve_categoria(record.servico)
        if not categoria:
            raise OmieApiError(
                "CATEGORIA",
                f"servico '{record.servico}' sem categoria mapeada — configurar em categoria_ids",
            )

        # Vencimento: AV = mesma data do servico, FA = ultimo dia do mes
        av = is_av_paid(record.av_pagamento)
        venc_iso = record.data if av else last_day_of_month(record.data)
        venc_br = _iso_para_br(venc_iso)

        param = {
            "codigo_lancamento_integracao": record.chave_deduplicacao,
            "codigo_cliente_fornecedor":    cliente_id,
            "data_vencimento":              venc_br,
            "valor_documento":              money_as_float(record.preco),
            "codigo_categoria":             categoria,
            "data_previsao":                venc_br,
            "id_conta_corrente":            self.id_conta_corrente,
            "observacao":                   build_history(record)[:500],
        }
        return self.client.request(
            "financas/contareceber", "IncluirContaReceber", param,
        )


def _iso_para_br(iso: str) -> str:
    """'2026-05-04' -> '04/05/2026'. Omie aceita só formato BR."""
    try:
        d = dt.date.fromisoformat(iso[:10])
        return d.strftime("%d/%m/%Y")
    except (ValueError, TypeError):
        return iso  # melhor passar lixo do que crashar; Omie vai reclamar
