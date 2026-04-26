# Astro Vistorias — Sistema Multi-Unidade

Plataforma web de gestão operacional + financeira da rede Astro Vistorias
(7 unidades hoje, meta 15 até 2027). Backend Flask + SQLite por unidade,
frontend HTML/CSS/JS vanilla, deploy no Railway, integração nativa com
Tiny ERP (financeiro) e Backblaze B2 (backup).

## O que faz

- **Caixa do Dia (PDV)** — lançamento individual à vista em tempo real
- **Fechamento de Caixa** — importa planilha do Sispevi, cruza com PDV,
  envia faturados ao Tiny. Inclui Wizard v2 com banner de alertas
  (match aproximado, duplicatas, AV sem PDV) e regras de conservação
- **Conferência Antecipada** — operadora carrega planilha durante o dia
  e vê status em tempo real do cruzamento com o PDV
- **Painel Master** — visão consolidada da rede (Barueri, Mooca, Moema,
  Indianópolis, São Miguel, Itu)
- **Contas a Receber** — dashboard alinhado com Tiny (emitidas,
  recebidas, em aberto, atrasadas, canceladas), agrupado por cliente
- **Royalties** — apuração mensal por unidade (Itu 7%, demais 3%)
- **Histórico Rede** — emissões Tiny + lançamentos PDV unificados em
  tabs com filtros por período/unidade
- **Backup B2** automático diário + test restore mensal + botão manual
- **Auditoria** — toda decisão sensível registrada com user + timestamp
  + motivo

## Stack

- **Backend**: Python 3.12 + Flask 3 + Gunicorn (2 workers)
- **DB**: SQLite por unidade (arquivo em `/data/<unit>/caixa.db`),
  migração automática de JSON legado
- **Frontend**: HTML/CSS/JS vanilla (sem build), Design System próprio
  (`frente_caixa/assets/design-system.css`)
- **Auth**: sessão Flask + CSRF token + PIN gerencial por unidade
  (pbkdf2 200k iter)
- **Integração**: Tiny ERP API v3 (OAuth 2.0 com refresh automático
  2×/dia), Backblaze B2 SDK
- **Deploy**: Railway (push to main → deploy automático)

## Rodar local

```bash
# 1. Setup ambiente
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Configurar .env (ver .env.example)
cp .env.example .env
# editar com suas credenciais Tiny/B2/etc

# 3. Rodar servidor
python3 server.py
# ou em modo produção:
gunicorn server:app --workers 2 --bind 0.0.0.0:8080
```

Abre em `http://localhost:8080`.

## Variáveis de ambiente essenciais

| Variável | Descrição | Onde setar |
|---|---|---|
| `SECRET_KEY` | Chave Flask pra sessão | Railway |
| `USERS_CONFIG` | JSON com usuários e permissões | Railway |
| `UNITS_CONFIG` | JSON com unidades base | Railway |
| `DATA_DIR` | Pasta de persistência (volume) | Railway: `/data` |
| `TINY_CLIENT_ID` / `_SECRET` | OAuth Tiny | Railway |
| `B2_KEY_ID` / `B2_APP_KEY` / `B2_BUCKET` | Backup | Railway |
| `MASTER_EMAIL` | Email com acesso master | Railway |

Ver `.env.example` pra lista completa. Detalhamento em
[GUIA_ENV.md](GUIA_ENV.md).

## Rodar testes

```bash
.venv/bin/python -m pytest tests/ -q
```

CI roda em todo push pra `main` via GitHub Actions
(`.github/workflows/tests.yml`). Status atual: **111/111 verdes**.

## Deploy

Push pra `main` → Railway redeploya automático. Se tocar caminho
crítico (caixa2.html, app.js, server.py rotas /caixa/*), considerar
janela de baixo movimento (domingo madrugada).

## Estrutura de pastas

```
.
├── server.py                 # Backend monolítico (8.7K linhas, 137 rotas)
├── tiny_import.py            # Cliente Tiny ERP (OAuth, lançamentos)
├── caixa_db.py               # SQLite por unidade
├── frente_caixa/             # Frontend (HTML/CSS/JS vanilla)
│   ├── caixa2.html           # PDV (caminho crítico)
│   ├── fechamento.html       # Fechamento + Wizard v2
│   ├── master.html           # Painel master
│   ├── app.js                # Lógica fechamento (2K linhas)
│   ├── caixa.js / caixa2.js  # Lógica PDV
│   ├── planilha-dia.js       # Conferência Antecipada
│   └── assets/
│       ├── app-shell.js      # Shell unificado v2.1
│       └── design-system.css # Tokens + componentes
├── tests/                    # pytest, 111 testes (100% passando)
├── docs/                     # Documentação técnica
├── data/                     # (volume Railway) /data/<unit>/...
└── .env.example              # Template de variáveis de ambiente
```

## Documentação adicional

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — visão de módulos, fluxos
  de dados, decisões importantes
- [docs/API.md](docs/API.md) — referência das 137 rotas agrupadas por
  domínio
- [GUIA_ENV.md](GUIA_ENV.md) — passo a passo das variáveis de ambiente
- [DOCUMENTACAO_TECNICA.md](DOCUMENTACAO_TECNICA.md) — premissas
  arquiteturais

## Status atual (26/04/2026)

- ✅ 4/6 unidades em produção com sistema completo (Barueri, Mooca,
  Moema, Itu parcial)
- ⏳ Indianópolis e São Miguel — Omie pendente (Etapa 2 do roadmap)
- ✅ Wizard de Fechamento v2 + Conferência Antecipada em prod
- ✅ 111/111 testes verdes + CI ativo
- ⏳ Cobrança Fase 2 (régua email) e Fase 3 (WhatsApp) na fila

## Ferramentas auxiliares (CLI antigo)

`tiny_import.py` ainda funciona como CLI standalone pra debug/dev local
(diagnóstico de tokens, listar IDs do Tiny, dry-run de envio):

```bash
python3 tiny_import.py --check-env
python3 tiny_import.py --list-formas-recebimento
python3 tiny_import.py --list-categorias
python3 tiny_import.py --auth-url
```

Em produção a integração Tiny roda direto pelo `server.py` (sem CLI).

## Convenções de commit

- `feat(<area>): ...` — nova funcionalidade
- `fix(<area>): ...` — correção
- `refactor(<area>): ...` — sem mudança de comportamento
- `style(<area>): ...` — visual/copy
- `test: ...` — testes
- `chore: ...` — config/build
- `perf(<area>): ...` — performance

Áreas comuns: `caixa`, `fechamento`, `master`, `unidades`, `tiny`,
`backup`, `cron`, `csrf`, `planilha-dia`, `wizard`, `bi`, `cobranca`.
