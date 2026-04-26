# API Reference — Astro Vistorias

131 rotas Flask agrupadas por domínio. Geradas automaticamente do
`server.py` em 2026-04-26.

**Convenções**:
- `/u/<unit>/...` — escopo de uma unidade (operador da unidade ou
  master/matriz)
- `/master/...` — apenas master (visão global)
- `/gerencial/...` — operadora gerencial da unidade
- `/api/...` — rotas globais (sem escopo de unidade)

**Auth**: tudo exige sessão Flask (`@login_required`) exceto `/login`,
`/api/csrf-token`, `/ativar/...` e arquivos estáticos.

**CSRF**: rotas `POST/PUT/DELETE/PATCH` exigem header `X-CSRF-Token`
(ver `getCsrfToken()` em `app.js`). Bypass automático em testes
(`app.testing=True`).

---

## Auth & Sessão

| Rota | Métodos | Descrição |
|---|---|---|
| `/login` | GET POST | Tela de login + autenticação |
| `/logout` | POST | Encerra sessão |
| `/esqueci-senha` | GET POST | Reset de senha por email |
| `/reset-senha/<token>` | GET POST | Trocar senha via token |
| `/ativar/<token>` | GET | Tela de ativação de convite |
| `/api/ativar/<token>` | GET POST | Validar/aceitar convite |
| `/api/csrf-token` | GET | Devolve token CSRF da sessão |
| `/api/me` | GET | Perfil + permissões do user logado |
| `/selecionar-unidade` | GET | Seleção de unidade (multi-acesso) |
| `/api/selecionar-unidade` | POST | Define unidade ativa na sessão |

---

## OAuth Tiny

| Rota | Métodos | Descrição |
|---|---|---|
| `/u/<unit>/tiny/oauth/start` | GET | Inicia fluxo OAuth Tiny |
| `/u/<unit>/tiny/oauth/callback` | GET | Callback do Tiny → grava tokens |
| `/u/<unit>/api/tiny/refresh` | POST | Força renovação manual de token |
| `/u/<unit>/api/tiny/test-call` | GET | Healthcheck (chama Tiny API) |

---

## Caixa do Dia (PDV)

| Rota | Métodos | Descrição |
|---|---|---|
| `/u/<unit>/caixa` | GET | Tela principal do PDV |
| `/u/<unit>/caixa2` | GET | Tela PDV v2 (ainda no shell antigo) |
| `/u/<unit>/api/caixa/lancar` | POST | Cria lançamento individual |
| `/u/<unit>/api/caixa/listar` | GET | Lista lançamentos do dia |
| `/u/<unit>/api/caixa/excluir/<id>` | DELETE | Remove lançamento |
| `/u/<unit>/api/caixa/editar/<id>` | POST | Edita lançamento existente |
| `/u/<unit>/api/caixa/conferir` | POST | Cruza records planilha × PDV (Wizard v2) |
| `/u/<unit>/api/caixa/totais` | GET | Totais por FP do dia |
| `/u/<unit>/api/caixa/estado` | GET | Snapshot completo do caixa atual |
| `/u/<unit>/api/caixa/abrir` | POST | Reabre caixa fechado (com PIN) |
| `/u/<unit>/api/caixa/fechar` | POST | Fecha o caixa do dia |

---

## Fechamento + Wizard v2

| Rota | Métodos | Descrição |
|---|---|---|
| `/u/<unit>/fechamento` | GET | Tela de fechamento (planilha + Wizard v2) |
| `/u/<unit>/api/preview` | POST | Pré-visualiza payload Tiny antes de enviar |
| `/u/<unit>/api/send` | POST | Envia FA pro Tiny (cria boletos) |
| `/u/<unit>/api/clear-imported` | POST | Limpa controle local de envios |
| `/u/<unit>/api/snapshot` | POST | Autosave do fechamento em andamento |
| `/u/<unit>/api/snapshot/load` | GET | Restaura snapshot |
| `/u/<unit>/api/fechamento/decisao` | POST | Registra decisão do Wizard v2 |
| `/u/<unit>/api/fechamento/relatorio` | GET | Relatório de decisões do dia |
| `/u/<unit>/api/divergencias/registrar` | POST | Loga divergência confirmada |

---

## Conferência Antecipada (planilha do dia)

| Rota | Métodos | Descrição |
|---|---|---|
| `/u/<unit>/api/planilha/upload` | POST | Persiste planilha do dia (versionado) |
| `/u/<unit>/api/planilha/dia` | GET | Devolve planilha persistida |
| `/u/<unit>/api/planilha/status` | GET | Cruzamento PDV em tempo real (stats + linhas) |

---

## Clientes Tiny (mapeamento)

| Rota | Métodos | Descrição |
|---|---|---|
| `/u/<unit>/api/suggest-clients` | POST | Sugere clientes Tiny por nome |
| `/u/<unit>/api/map-client` | POST | Mapeia cliente local ↔ Tiny |
| `/u/<unit>/api/auto-map-clients` | POST | Mapeamento automático em lote |

---

## Gerencial da Unidade

| Rota | Métodos | Descrição |
|---|---|---|
| `/u/<unit>/gerencial-unidade` | GET | Tela gerencial |
| `/u/<unit>/api/gerencial-unidade` | GET | Dados (FP do dia, top clientes) |
| `/u/<unit>/api/gerencial-unidade/exportar` | GET | Exportar histórico |

---

## Unidade — admin (master)

| Rota | Métodos | Descrição |
|---|---|---|
| `/master/unidades` | GET | Tela de gestão de unidades |
| `/master/api/unidades` | GET POST | Listar / criar unidade |
| `/master/api/unidades/<slug>` | DELETE | Remover unidade |
| `/master/api/unidades/<slug>/pin` | POST | Definir/trocar PIN gerencial |
| `/master/api/unidades/<slug>/limpar-dia` | POST | Limpa dia (envios + caixa + fechamento) |
| `/master/api/unidades/<slug>/formas-recebimento` | GET POST | Mapear FP local ↔ Tiny |

---

## Painel Master

| Rota | Métodos | Descrição |
|---|---|---|
| `/master` | GET | Painel master (chips por unidade) |
| `/master/api/units-status` | GET | Status agregado das unidades |
| `/master/api/tiny-health` | GET | Saúde dos tokens Tiny |
| `/master/api/diag/tokens` | GET | Diagnóstico detalhado de tokens |
| `/master/api/duplicados-envios` | GET | Detecta envios duplicados |
| `/master/api/usuarios-conectados` | GET | Quem está logado agora |

---

## Sistema (saúde + manutenção)

| Rota | Métodos | Descrição |
|---|---|---|
| `/master/sistema` | GET | Tela de saúde + manutenção |
| `/master/api/sistema/saude` | GET | Health agregado (uptime, disco, Tiny, backup, JS, crons) |
| `/master/api/backup/now` | POST | Dispara backup manual |
| `/master/api/backup/status` | GET | Último backup + log |
| `/master/api/backup/test-restore` | POST | Roda test restore on-demand |
| `/master/api/js-errors` | GET | Lista erros frontend recentes |
| `/master/api/debug/storage` | GET | Diagnóstico de uso de disco |
| `/master/api/debug/email-test` | POST | Testa envio de email SMTP |
| `/api/log/js-error` | POST | Recebe erros JS do frontend (sendBeacon) |

---

## Categorias / aliases / mapeamentos (master)

| Rota | Métodos | Descrição |
|---|---|---|
| `/master/categorias` | GET | Tela de categorias |
| `/master/api/categorias/<unit>` | GET POST | CRUD categorias da unidade |
| `/master/api/categorias/<unit>/<path:nome>` | DELETE | Remove categoria |
| `/master/api/categorias/<unit>/importar-tiny` | POST | Importa categorias do Tiny |

---

## Auditoria + Aprovações

| Rota | Métodos | Descrição |
|---|---|---|
| `/master/auditoria` | GET | Tela de auditoria |
| `/master/api/auditoria` | GET | Lista de eventos |
| `/master/api/auditoria.csv` | GET | Export CSV |
| `/master/aprovacoes` | GET | Tela de fila de aprovações |
| `/master/api/aprovacoes` | GET POST | Listar/criar aprovação |
| `/master/api/aprovacoes/<approval_id>` | POST | Aprovar/rejeitar |

---

## Usuários + Convites

| Rota | Métodos | Descrição |
|---|---|---|
| `/master/usuarios` | GET | Tela de gestão de usuários |
| `/master/api/usuarios` | GET POST | Listar/criar usuário |
| `/master/api/usuarios/<email>` | DELETE | Remover usuário |
| `/master/api/usuarios/<email>` | POST | Editar permissões |
| `/master/api/convites` | GET POST | Lista/cria convite |
| `/master/api/convites/<token>` | DELETE | Revoga convite |
| `/master/api/convites/<token>/reenviar` | POST | Reenvia email do convite |
| `/master/api/convites.csv` | GET | Export CSV |

---

## Contas a Receber

| Rota | Métodos | Descrição |
|---|---|---|
| `/master/contas-receber` | GET | Dashboard contas a receber |
| `/master/api/contas-receber` | GET | Dados (cache 5min) |

---

## Royalties

| Rota | Métodos | Descrição |
|---|---|---|
| `/master/royalties` | GET | Tela de royalties por unidade |
| `/master/api/royalties` | GET | Dados (Itu 7%, demais 3%) |

---

## Histórico Rede (unificado)

| Rota | Métodos | Descrição |
|---|---|---|
| `/master/historico` | GET | Tela com tabs Emitidos/PDV/BI |
| `/master/api/historico` | GET | Dados unificados |
| `/master/api/inadimplencia.csv` | GET | Export CSV de inadimplência |

---

## BI / Faturamento

| Rota | Métodos | Descrição |
|---|---|---|
| `/gerencial/bi` | GET | Tela BI (legacy, absorvido pelo histórico) |
| `/gerencial/api/bi/faturamento` | GET | Faturamento por dia × categoria |
| `/gerencial/api/bi/historico-emitido` | GET | Emitidos no Tiny |
| `/gerencial/api/bi/sync-historico` | POST | Sincroniza com Tiny |
| `/gerencial/api/bi/debug-tiny/<unit>` | GET | Debug payload Tiny |

---

## Cobrança (Fase 1 — boletos)

| Rota | Métodos | Descrição |
|---|---|---|
| `/master/cobranca` | GET | Tela de cobrança |
| `/master/api/cobranca/regua` | GET | Status da régua (Fase 2/3 pendentes) |

---

## Backup + Histórico

| Rota | Métodos | Descrição |
|---|---|---|
| `/gerencial/api/backup` | POST | Backup manual da unidade |
| `/gerencial/api/backup/download` | GET | Baixa último backup |
| `/gerencial/api/historico` | GET | Histórico antigo (legacy) |
| `/gerencial/api/divergencias` | GET | Divergências registradas |
| `/gerencial/api/exportar` | GET | Export geral |
| `/gerencial/api/envios-tiny` | GET | Lista envios Tiny |
| `/gerencial/api/envios-tiny/migrate` | POST | Migração legacy |
| `/gerencial/historico-caixa` | GET | Tela legacy |
| `/gerencial/historico-emitido` | GET | Tela legacy |

---

## Páginas estáticas

| Rota | Descrição |
|---|---|
| `/` | Home / redirect login |
| `/<path:filename>` | Arquivos estáticos do `frente_caixa/` |
| `/manual` | Manual operacional (HTML) |
| `/master/roadmap` | Tela do roadmap |

---

## Códigos de erro padrão

| Código | Significado |
|---|---|
| 200 | OK |
| 400 | Validação (campo faltando, formato inválido) |
| 401 | Não autenticado |
| 403 | Sem permissão / CSRF inválido / PIN inválido |
| 404 | Recurso não encontrado |
| 429 | Rate limit (PIN, login) |
| 500 | Erro interno (logado em `app.logger.exception`) |

Body padrão de erro:
```json
{ "success": false, "error": "mensagem humana", "code": "opcional_machine_code" }
```

Códigos especiais (`code`):
- `pin_required` — endpoint exige PIN no body
- `pin_invalid` — PIN não confere
- `session_expired` — sessão expirou, redirect pra login
- `tiny_oauth_expired` — token Tiny expirou, reautorizar

---

## Padrão de resposta JSON

Endpoints REST sempre retornam `{success: bool, ...}`:

```json
// sucesso
{"success": true, "data": {...}, "total_decisoes_dia": 3}

// erro
{"success": false, "error": "data invalida (YYYY-MM-DD)", "code": "..."}
```

Exceções: endpoints HTML retornam template Jinja, não JSON.
