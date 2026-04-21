# Plano de Manutenção — 22/04/2026

**Status**: Sistema em manutenção o dia inteiro. Unidades retomam operação **23/04/2026 às 09:00**.

**Contexto**: Primeiro dia de operação real foi 21/04/2026 (Barueri e Mooca). Apareceram 2 tipos de erro: cruzamento de placa que não bateu entre PDV e planilha, e falhas de autenticação Tiny. Essa manutenção corrige + adiciona prevenção.

---

## Janela do dia

| Bloco | Horário | O que rola |
|---|---|---|
| Preparação | 08:00 – 09:00 | Backup, manutenção ligada |
| Etapa 1 | 09:00 – 10:00 | Investigação do cruzamento |
| Etapa 2 | 10:00 – 11:30 | Alerta Tiny expirando |
| Almoço | 11:30 – 13:00 | |
| Etapa 3 | 13:00 – 16:00 | Tabela `envios_tiny` (a maior) |
| Etapa 4 | 16:00 – 17:00 | Tela de histórico |
| Etapa 5 | 17:00 – 18:00 | Painel de divergências |
| Validação | 18:00 – 20:00 | Smoke test, ajustes, encerrar |

Total: ~10h dedicadas + 2h de buffer.

---

## ETAPA 0 — Preparação (08:00 – 09:00)

- [ ] 0.1 Confirmar que unidades sabem: 22/04 sistema em manutenção, volta 23/04 9h
- [ ] 0.2 Rodar backup download (`/gerencial/api/backup/download`) e salvar zip no PC antes de qualquer mudança
- [ ] 0.3 Ligar "modo manutenção" (página estática "Sistema em manutenção — voltamos 23/04 9h")
- [ ] 0.4 Rodar `/master/api/debug/storage` e anotar estado inicial

**Gate**: zip no PC + manutenção ligada. Só aí avança.

---

## ETAPA 1 — Investigação do cruzamento de placa (09:00 – 10:00)

**Objetivo**: descobrir por que a placa de 21/04 não bateu e corrigir (alias faltando ou bug).

- [ ] 1.1 Ian fornece: placa exata, nome do serviço no PDV, nome na planilha, unidade
- [ ] 1.2 SQL direto no banco da unidade pra confirmar
- [ ] 1.3 Testar algoritmo de matching com os dois nomes
- [ ] 1.4 Diagnóstico: alias faltando? bug? digitação?
- [ ] 1.5 Correção:
    - Alias faltando: adicionar em `UNITS_CONFIG` (env var Railway) — 5 min
    - Bug no código: patch + commit
- [ ] 1.6 Teste: rodar cruzamento daquele par de novo, confirmar que bate

**Gate**: placa de teste cruza.

**Rollback**: reverter env var ou commit.

---

## ETAPA 2 — Alerta preventivo de Tiny expirando (10:00 – 11:30)

**Objetivo**: avisar o operador ANTES do Tiny cair por refresh_token expirado.

- [ ] 2.1 Design apresentado:
    - Verificação a cada 30 min no cron existente
    - Se `access_token` vai expirar em < 2h ou já expirou: email pra recebedor do backup + badge vermelho no `/home` master
    - Opcional: tentar refresh automático; se falhar, alerta
- [ ] 2.2 Ian autoriza 100%
- [ ] 2.3 Implementar
- [ ] 2.4 Teste local: forçar token vencido, confirmar disparo
- [ ] 2.5 Commit + deploy

**Gate**: badge aparece com token vencido, some quando renova. Email de alerta chega.

**Rollback**: reverter commit.

---

## ETAPA 3 — Tabela `envios_tiny` em SQLite (13:00 – 16:00)

**Objetivo**: substituir o `imported.json` por tabela SQLite. Base pra reenviar + contas a receber.

**⚠️ Etapa mais pesada. Executada em 2 fases.**

### Fase 3A — Modo espelho (13:00 – 14:30)

Tabela grava em paralelo ao `imported.json`. Zero risco.

- [ ] 3A.1 Apresentar schema (colunas, índices)
- [ ] 3A.2 Ian autoriza 100%
- [ ] 3A.3 Criar tabela + DDL em `caixa_db.py`
- [ ] 3A.4 No `api_send`, gravar na tabela a cada envio (sucesso, falha, duplicado) — sem mudar lógica do JSON
- [ ] 3A.5 Migrar `imported.json` atual pra tabela (script one-shot)
- [ ] 3A.6 Deploy
- [ ] 3A.7 Teste: envio dummy, confirmar que caiu na tabela

### Fase 3B — Troca da fonte de verdade (14:30 – 16:00)

Tabela vira a fonte. `imported.json` segue como backup por 1 semana.

- [ ] 3B.1 Trocar checagem "já enviado?" pra ler da tabela
- [ ] 3B.2 Teste: fechar caixa fictício com 2 placas, enviar, reimportar → segunda vez pula
- [ ] 3B.3 Commit + deploy
- [ ] 3B.4 Validar com dado real (só leitura, sem re-enviar)

**Gate**: tabela tem todos os registros + teste de dedup via tabela.

**Rollback 3A**: reverter commit. `imported.json` intacto.
**Rollback 3B**: reverter commit pra voltar leitura pro JSON.

---

## ETAPA 4 — Tela de histórico por período (16:00 – 17:00)

**Objetivo**: UI pra consultar lançamentos de qualquer período. Rota API já existe.

- [ ] 4.1 Apresentar mockup (filtro de data, lista, tabela)
- [ ] 4.2 Ian autoriza
- [ ] 4.3 Criar `historico-caixa.html` + link no menu
- [ ] 4.4 Teste: abrir, filtrar últimos 7 dias, conferir

**Gate**: tela carrega, exporta CSV opcional.

**Rollback**: remover arquivo + link.

---

## ETAPA 5 — Painel de divergências acumuladas (17:00 – 18:00)

**Objetivo**: ver padrões de divergência ao longo do tempo.

- [ ] 5.1 Design: `/u/<unit>/divergencias` com filtro de período + agregação por motivo/serviço/FP
- [ ] 5.2 Ian autoriza
- [ ] 5.3 Implementar
- [ ] 5.4 Teste com divergências existentes

**Gate**: painel mostra números coerentes.

**Rollback**: remover arquivos.

---

## VALIDAÇÃO FINAL (18:00 – 20:00)

- [ ] V.1 Rodar backup download (estado pós-manutenção)
- [ ] V.2 Smoke test completo simulando 23/04 9h:
    - Login como operador em Barueri
    - Lançamento PDV manual
    - Importar planilha (de ontem ou fictícia)
    - Cruzamento, envio Tiny (se seguro)
    - Fechamento
    - Logout
- [ ] V.3 Conferir: histórico mostra lançamento, tabela registrou, nenhum erro no Sentry
- [ ] V.4 Desligar modo manutenção
- [ ] V.5 Avisar unidades: liberado
- [ ] V.6 Backup final com nome `22-04-apos-manutencao.zip`

**Gate final**: tudo verde OU reverter etapa problemática e abrir 23/04 sem ela.

---

## Regras de segurança ao longo do dia

- Commit atômico por etapa — cada etapa é 1 commit, fácil de reverter isoladamente
- Backup download entre etapas pesadas (antes e depois da Etapa 3 em especial)
- Nenhum envio Tiny real em teste — usar flag de dry-run ou banco vazio
- Se travar num ponto, para, alinha, e só segue com 100%

---

## Pré-requisitos a entregar 22/04 às 08:00

1. Placa exata + nomes dos serviços (PDV e planilha) do caso que não cruzou em 21/04
2. Confirmação de qual email recebe o backup de `noreply@astrovistorias.com.br`
3. Se o backup do dia 21/04 chegou nesse email
4. Acesso Railway validado (deploy automático via git push está OK)

---

## Registro de execução

Preencher conforme for executando. Data/hora + resultado + observações.

| Etapa | Início | Fim | Status | Observações |
|---|---|---|---|---|
| 0 | | | | |
| 1 | | | | |
| 2 | | | | |
| 3A | | | | |
| 3B | | | | |
| 4 | | | | |
| 5 | | | | |
| Validação | | | | |
