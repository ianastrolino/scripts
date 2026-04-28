# Arquitetura — Astro Vistorias

Visão de módulos, fluxos de dados e decisões importantes do sistema.

## Visão geral

Sistema **monolítico Flask multi-tenant**, com isolamento por unidade
(prefixo `/u/<unit>` em URLs e diretório próprio em `/data/<unit>`).

```
┌─────────────────────────────────────────────────────────────┐
│  Browser (Chrome em desktop nas 7 unidades)                 │
│  ├─ caixa2.html (PDV)                                       │
│  ├─ fechamento.html (Wizard v2)                             │
│  └─ master.html (admin global)                              │
└──────────────────┬──────────────────────────────────────────┘
                   │ HTTPS
┌──────────────────▼──────────────────────────────────────────┐
│  Railway — Gunicorn (2 workers)                             │
│  └─ server.py (Flask, 137 rotas)                            │
│     ├─ /u/<unit>/api/caixa/*       (PDV)                    │
│     ├─ /u/<unit>/api/fechamento/*  (decisões + relatório)   │
│     ├─ /u/<unit>/api/planilha/*    (Conf Antecipada)        │
│     └─ /master/api/*               (rede)                   │
└──┬────────────────────┬────────────────────┬────────────────┘
   │                    │                    │
   ▼                    ▼                    ▼
┌──────────┐      ┌──────────┐         ┌──────────┐
│ /data/   │      │ Tiny ERP │         │ Backblaze│
│ <unit>/  │      │ API v3   │         │ B2       │
│ ├─ caixa │      │ (OAuth)  │         │ (backup) │
│ │ .db    │      └──────────┘         └──────────┘
│ ├─ jsons │
│ └─ logs  │
└──────────┘
```

## Módulos principais

### `server.py` (8.7K linhas, 137 rotas)

Coração da aplicação. Centraliza:
- Bootstrap Flask (config, hooks, error handlers)
- Decoradores: `login_required`, `csrf_required`, `master_only_required`,
  `unit_access_required`
- Autenticação: sessão Flask + login/logout + reset senha
- PIN gerencial: pbkdf2 200k iter, hash em `/data/pins.json`, rate limit
  por IP
- Crons em thread separada (`_cron_loop`):
  - 00:00 → backup B2
  - 02:30 → rotação de logs por idade
  - 07:00 + 15:00 → renovação proativa de tokens Tiny
  - 18:30 → alerta tokens próximos de expirar
  - dia 1 03:00 → test restore B2
- Multi-worker state: lock + reload via `mtime` (evita workers vendo
  estados divergentes)
- Endpoints REST agrupados por domínio (ver
  [docs/API.md](API.md))

**Por quê monolito?** Foi crescimento orgânico com 1 dev. Testes
existentes (111 verdes) protegem regression — refactor pra blueprints
está planejado (Etapa 4 do hardening).

### `tiny_import.py`

Cliente Tiny ERP API v3:
- OAuth 2.0 (client credentials + refresh token automático)
- `create_accounts_receivable()` — cria boletos no Tiny
- Idempotência via `numero_documento` único
- Listagens: clientes, formas de recebimento, categorias
- Padronização de dados (aliases de serviços, FPs, clientes)
- CLI standalone pra debug local (`--check-env`, `--list-*`,
  `--exchange-code`, `--oauth-local`)

**Estado**: `state/tiny_tokens.json` por unidade

### `caixa_db.py`

SQLite por unidade. Schema:
- `lancamentos` (id, placa, cliente, servico, valor, fp, hora, ts, ...)
- Índices em placa + data
- Migração automática de JSON legado (`caixa_dia.json` → SQLite)
- Funções: `load_lancamentos`, `add_lancamento`, `delete_lancamento`,
  `update_lancamento`, `migrate_from_json`

**Por quê SQLite por unidade?** Isolamento total, backup simples (1
arquivo), sem RDBMS pra manter. Suficiente pra ~500 lançamentos/dia/unidade.

### Frontend (`frente_caixa/`)

HTML/CSS/JS vanilla, sem build step. Cada tela é HTML standalone.

**Shell v2.1** (`assets/app-shell.js` + `design-system.css`):
- NAV_CATALOG por perfil (operador / gerencial / matriz / master)
- Captura erros JS globais → POST `/api/log/js-error`
- Throttle 30 errors/sessão, dedup
- Tema dark/light persistente

**Telas críticas**:
- `caixa2.html` (90KB) — PDV com lançamento individual + filtro busca +
  totais por FP. **Ainda no shell antigo** (migração planejada Etapa 4.2).
- `fechamento.html` (85KB) — importa planilha XLS, cruza com PDV,
  Wizard v2 com banner alertas + card Conservação + decisões
  persistidas. Shell v2.1.
- `master.html` — painel da rede com chips Aberto/Conferindo/Fechado
  por unidade

**Scripts JS principais**:
- `app.js` (2K linhas) — lógica do fechamento (parser planilha,
  cruzamento, banner alertas, modal PIN/FP)
- `caixa.js` + `caixa2.js` + `caixa2-init.js` — lógica PDV
- `planilha-dia.js` — Conferência Antecipada (isolado, sem acoplar ao
  caixa2 critical path)

## Fluxos de dados

### Lançamento PDV (PDV → DB)

```
Operadora preenche form (#fPlaca, #fServico, #fValor, FP)
  → POST /u/<unit>/api/caixa/lancar
  → server.py valida + insere em SQLite
  → broadcast pra todos workers (multi-worker reload)
  → response JSON com lancamento + totais atualizados
  → frontend atualiza chips + tabela "Ao vivo"
```

### Fechamento de caixa (Planilha + PDV → Tiny)

```
1. Operadora exporta planilha do Sispevi (HTML/XLS)
2. Drag&drop em fechamento.html
   → app.js parseExportedHtml() → array de records
3. POST /u/<unit>/api/caixa/conferir com records
   → server.py cruza placa+servico com PDV do dia
   → retorna conferencia + v2 (alertas: match aprox, duplicatas, sem PDV)
4. UI mostra Wizard v2:
   - Tabs AV/FA/DETRAN/Pendências
   - Chips de status por linha
   - Banner alertas com botões (Aplicar, Cortesia c/ PIN, Faturar, etc)
   - Card Conservação ("Planilha 12/12 identificadas")
5. Decisões persistidas via POST /api/fechamento/decisao
   → /data/<unit>/fechamento_decisoes.json
6. POST /u/<unit>/api/preview com records prontos
   → server.py monta payload Tiny (idempotência via numero_documento)
7. POST /u/<unit>/api/send
   → tiny_import.py cria boletos via OAuth
   → grava sucesso em /data/<unit>/imported.json
   → frontend marca linhas como ✓ enviado
```

### Conferência Antecipada (status em tempo real)

```
1. Operadora exporta planilha durante o dia (várias vezes)
2. Painel "Planilha do dia" no caixa2.html (planilha-dia.js)
3. Multi-upload: parseia N arquivos, combina records, manda 1 POST
4. POST /u/<unit>/api/planilha/upload
   → /data/<unit>/planilha_dia/<YYYY-MM-DD>.json (versionado, diff)
5. GET /u/<unit>/api/planilha/status (auto a cada 2min visível)
   → cruza com PDV em tempo real
   → retorna stats (total, cruzadas, divergencias, orfas, dia anterior)
6. UI atualiza chips + bloco detalhes
7. Botão "+ Lançar agora" pré-preenche form do PDV
```

### Backup B2 (cron diário)

```
00:00 SP → _cron_loop dispara _executar_backup()
  → dedup: checa backup_log; se já rodou hoje, pula
  → tar.gz de /data/<unit>/* pra cada unidade
  → upload pra B2 (bucket por env var)
  → grava sucesso/falha em backup_log.jsonl
  → 18:30 SP → alerta se backup do dia falhou
```

### OAuth Tiny (renovação automática)

```
07:00 + 15:00 SP → _cron_loop dispara _renovar_tokens_tiny()
  → pra cada unidade Tiny ativa:
     - lê tokens atuais
     - chama /openid-connect/token com refresh_token
     - persiste novo access_token + refresh_token
     - log em token_refresh.jsonl
  → falha → alerta via /master/api/sistema/saude
```

## Decisões arquiteturais importantes

### 1. **Planilha = fonte da verdade**

O que vem do Sispevi é autoritativo. PDV complementa (forma de pagamento
quando AV genérico). Total final ≥ total planilha (extras só somam,
nunca subtraem). Ver `feedback_fechamento_conservacao.md` na memória do
Claude.

### 2. **Sistema só alerta, operador decide**

Wizard v2 nunca apaga ou altera lançamentos por conta própria. Banner
mostra divergências, operador escolhe ação (Aplicar match, Cortesia,
Faturar, Ignorar). Cada decisão fica logada com user + motivo +
timestamp.

### 3. **Cortesia exige PIN, demais não**

Operador pode resolver qualquer problema do fechamento (sem barreira),
mas Cortesia (dispensar pagamento) exige PIN gerencial. Decisão
financeira sensível.

### 4. **Multi-worker state via mtime reload**

Gunicorn 2 workers = 2 processos. State em memória (`UNITS`, tokens)
seria divergente. Solução: cada save toca arquivo, cada read checa
mtime, recarrega se mudou. Reload barato (poucos arquivos pequenos).

### 5. **CSRF rotativo + retry automático**

Token CSRF na sessão. `apiFetch()` no frontend cacheia o token. Se 403
+ erro CSRF, refaz GET /api/csrf-token e retenta automaticamente.
Operadora não vê erro.

### 6. **Marker visual "✏ tocado"**

Toda linha do fechamento que recebe intervenção do operador (Faturar,
Cortesia, Adicionar pagamento, Ignorar manual) ganha chip roxo com
tooltip de auditoria. Auditável visualmente sem abrir log.

### 7. **Conferência Antecipada vs Fechamento**

Mesmo motor de cruzamento (placa+serviço, fallback placa+valor,
detecção de duplicatas), aplicado em momentos diferentes:
- **Conf Antecipada** (caixa2.html, planilha-dia.js) — DURANTE o dia,
  status visual + click-to-fill, planilha pode ser re-uploaded N vezes
- **Wizard v2** (fechamento.html, app.js) — FIM do dia, decisões
  persistidas, envio Tiny

Backend tem 2 endpoints distintos (`/api/caixa/conferir` para
fechamento, `/api/planilha/status` para Conf Antecipada) com lógica
similar mas independente. Refactor pra extrair motor único está
planejado.

### 8. **Itu = caso especial**

Royalties 7% (vs 3% das demais). Tiny não autorizado ainda. Sistema
trata como unidade ativa mas com flags `nao_aplicavel` em saúde Tiny.

### 9. **Indianópolis + São Miguel = Omie**

Não usam Tiny. Integração Omie pendente (Etapa 2 roadmap). Hoje só
operam o PDV local.

## Backup e recuperação

- **Backup diário B2**: 00:00 SP via cron interno. Dedup contra
  duplicação se workers reiniciarem.
- **Test restore mensal**: dia 1 03:00 SP. Baixa ZIP do B2, valida
  SQLite efêmero, reporta no dashboard.
- **Botão manual** em /master/sistema (test restore on-demand).
- **Snapshot fechamento**: a cada save no fechamento, autosave em
  `/data/<unit>/snapshot_fechamento.json`. Restaurável.

## Observabilidade

### Healthcheck público (`GET /health`)

Endpoint sem autenticação consumido pelo Railway pra decidir se restarta
o pod (configurado em `railway.json`: `healthcheckPath: /health`,
`timeout: 30s`, `restartPolicyType: ON_FAILURE`).

Verifica:
- **app**: processo vivo (sempre OK se chegou aqui)
- **db**: SQLite acessível (mede `latency_ms`)
- **disk**: ≥ 5% livre (crítico) — warning < 10%
- **fs_write**: filesystem aceita writes (volume montado, sem read-only fs)
- **tokens**: contagem de tokens Tiny ativos (warning-only — não derruba)

Retorna:
- `200` quando `status ∈ {"ok", "degraded"}` — Railway considera saudável
- `503` quando `status == "unhealthy"` — Railway dispara restart

Cache 10s pra reduzir overhead quando bateado por monitoramento externo +
Railway simultaneamente.

### Dashboard `/master/sistema`
- **JS errors**: `window.error` + `unhandledrejection` capturados →
  `sendBeacon` → `/api/log/js-error` → `js_errors.jsonl`
- **Audit log**: `audit_log.jsonl` em `/data/audit/` — toda ação
  sensível (criar/remover unidade, definir PIN, limpar dia, aprovar
  fechamento)
- **Session log**: `session_log.jsonl` por unidade — login/logout/
  troca de unidade

## Pontos de evolução

- **Refactor server.py em blueprints** (Etapa 4 hardening) — 8.7K
  linhas em 1 arquivo é dor crescente
- **Migração caixa2.html → shell v2.1** (Etapa 5 hardening) — único
  shell antigo restante
- **Testes mais profundos** — Tiny import + master routes + OAuth
  flow têm cobertura mínima hoje
- **Métricas time-series** — tudo é evento pontual em JSONL.
  Dashboard de tendência exige adicionar Prometheus ou similar
- **Cobrança Fase 2/3** — régua email + WhatsApp ainda pendentes
- **Sistema de tickets** — operadoras hoje reportam por WhatsApp
  pessoal
