# Comparativo de padronizacao para inclusao no Tiny

Este documento mostra o que precisa ficar padronizado para a importacao criar contas a receber corretamente no Tiny/Olist.

## Resumo do fluxo

A planilha diaria traz dados de servicos realizados. O script transforma somente as linhas `FA` em lancamentos de contas a receber no Tiny. As linhas `AV` ficam para o fechamento do caixa/painel, porque podem ser dinheiro, cartao de credito, cartao de debito ou Pix.

O painel pode receber mais de uma planilha no mesmo lote, por exemplo laudos cautelares/verificacao e vistoria de transferencia. Todas entram no mesmo formato interno de conferencia.

O Tiny nao recebe o nome do cliente solto no financeiro. Ele precisa do `contato.id`. Por isso, o ponto mais importante da padronizacao e transformar o nome do cliente da planilha em um contato valido no Tiny.

## Comparativo campo a campo

| Campo na planilha | Exemplo na planilha | Campo no Tiny | Regra de padronizacao | Obrigatorio para enviar? | Onde configurar |
|---|---:|---|---|---|---|
| Data | `13/04/2026` | `data`, `dataVencimento`, `dataCompetencia` | Converter para `YYYY-MM-DD`. Para `FA`, o vencimento padrao e o ultimo dia do mes. | Sim para `FA` | `.env`: `TINY_VENCIMENTO_TIPO`, `TINY_VENCIMENTO_DIAS` |
| Cliente | `MARIN IMPORT` | `contato.id` | Encontrar contato no Tiny pelo nome ou mapear manualmente nome -> ID. Evitar criar duplicado. | Sim | `.env`: `TINY_CLIENTE_IDS_JSON`, `TINY_CLIENTE_ALIASES_JSON`, `TINY_AUTO_CREATE_CONTACTS` |
| Servico | `LAUDO CAUTELAR VERI` | `historico` | Corrigir nomes truncados antes de montar o historico. Nao cria produto/servico no Tiny. | Sim, como historico | `.env`: `TINY_SERVICO_ALIASES_JSON` |
| FP | `AV`, `FA` | Regra de envio e `formaRecebimento` | `FA` envia para contas a receber. `AV` fica no painel/caixa e nao entra no contas a receber por padrao. | Sim | `.env`: `TINY_CONTAS_RECEBER_FP_JSON`, `TINY_FORMA_RECEBIMENTO_IDS_JSON`, `TINY_FP_ALIASES_JSON` |
| Preco | `R$ 200,00` | `valor` | Converter moeda brasileira para decimal, exemplo `200.00`. | Sim | Automatico |
| Modelo | `I/PORSCHE 911 CARRERA` | `historico` | Manter em maiusculo e incluir no historico para identificar o veiculo. | Recomendado | Automatico |
| Placa | `FBA1I22` | `historico` | Remover espacos/pontos/tracos e manter letras/numeros em maiusculo. | Recomendado | Automatico |
| Arquivo/linha | `14_04_2026.xls`, linha `2` | `numeroDocumento`, `historico`, controle interno | Criar numero unico: `PLANILHA-YYYYMMDD-LINHA`. Tambem gera chave de deduplicacao. | Sim | `.env`: `TINY_NUMERO_DOCUMENTO_PREFIX` |
| Categoria financeira | Receita de laudos, por exemplo | `categoria.id` | Usar ID da categoria de receita, se voces usam categoria no financeiro. | Opcional, mas recomendado | `.env`: `TINY_CATEGORIA_ID` |

## Regras atuais do script

| Regra | Como esta hoje |
|---|---|
| Tipo de lancamento | Conta a receber: `POST /contas-receber` |
| Uma linha `FA` da planilha vira | Um lancamento em aberto no contas a receber |
| Uma linha `AV` da planilha vira | Um item de conferencia do caixa/painel, sem envio ao contas a receber por padrao |
| Campos usados da planilha | `data`, `modelo`, `placa`, `cliente`, `servico`, `FP`, `preco` |
| Campos ignorados | Numero, ano, cor, chassi, perito, digitador, preco sugerido, gravacao, subtotais e rodapes |
| Data de vencimento FA | Ultimo dia do mes da data da planilha |
| Historico | `SERVICO | MODELO | Placa PLACA | FP FP | Origem arquivo linha X` |
| Duplicidade | Controlada por `state/imported.json` depois do envio |
| Envio real | So acontece com `--send` |
| Teste seguro | Sem `--send`, o script so gera CSV/JSON/preview |

## Padroes que precisam ficar definidos

### 1. Clientes

O Tiny precisa do ID do contato. Hoje a planilha traz somente o nome.

Clientes encontrados na planilha de exemplo:

| Nome na planilha | Situacao atual | Regra recomendada |
|---|---|---|
| `2B MOTORS` | Nao encontrado por nome exato no Tiny | Cadastrar contato no Tiny ou mapear para ID existente |
| `CAR CHASE` | Nao encontrado por nome exato no Tiny | Cadastrar contato no Tiny ou mapear para ID existente |
| `CMR VEICULOS LTDA` | Nao encontrado por nome exato no Tiny | Cadastrar contato no Tiny ou mapear para ID existente |
| `EUROPAMOTORS` | Nao encontrado por nome exato no Tiny | Cadastrar contato no Tiny ou mapear para ID existente |
| `MARIN IMPORT` | Nao encontrado por nome exato no Tiny | Cadastrar contato no Tiny ou mapear para ID existente |
| `PARK MOTORS` | Nao encontrado por nome exato no Tiny | Cadastrar contato no Tiny ou mapear para ID existente |
| `PARTICULAR MOEMA` | Nao encontrado por nome exato no Tiny | Cadastrar contato no Tiny ou mapear para ID existente |
| `ZUFFEN MOTORS` | Nao encontrado por nome exato no Tiny | Cadastrar contato no Tiny ou mapear para ID existente |

Exemplo de mapeamento manual:

```env
TINY_CLIENTE_IDS_JSON={"MARIN IMPORT":123456789,"EUROPAMOTORS":123456790}
```

Exemplo quando o nome da planilha e diferente do cadastro:

```env
TINY_CLIENTE_ALIASES_JSON={"EUROPAMOTORS":"EUROPA MOTORS LTDA"}
```

Recomendacao inicial: manter `TINY_AUTO_CREATE_CONTACTS=false` ate conferir os nomes, para evitar duplicidade.

### 2. Formas de recebimento

Na planilha aparecem:

| FP da planilha | Significado provavel | Precisa virar |
|---|---|---|
| `AV` | A vista: dinheiro, cartao credito, cartao debito ou Pix | Nao enviar para contas a receber por padrao; tratar no painel de caixa |
| `FA` | A faturar | Enviar para contas a receber em aberto com vencimento no ultimo dia do mes |

Formas encontradas no Tiny:

| ID | Nome no Tiny |
|---:|---|
| `556498207` | `Dinheiro` |
| `556498209` | `Cartao de credito` |
| `556498211` | `Cartao de debito` |
| `556498213` | `Boleto` |
| `556498217` | `Deposito` |
| `802165201` | `A faturar` |
| `802165265` | `Cortesia` |
| `803887338` | `Retorno` |

Regra ativa de envio ao contas a receber:

```env
TINY_CONTAS_RECEBER_FP_JSON=["FA"]
TINY_VENCIMENTO_TIPO=ultimo_dia_mes
```

Mapeamento provavel para `FA`:

```env
TINY_FORMA_RECEBIMENTO_IDS_JSON={"FA":802165201}
```

`AV` sera detalhado no painel, porque a planilha nao informa se foi dinheiro, cartao de credito, cartao de debito ou Pix.

### 3. Servicos

A exportacao antiga corta alguns nomes. O script ja corrige os principais:

| Como vem na planilha | Como fica padronizado |
|---|---|
| `LAUDO DE VERIFICACA` | `LAUDO DE VERIFICACAO` |
| `LAUDO CAUTELAR VERI` | `LAUDO CAUTELAR VERIFICACAO` |
| `CAUTELAR COM ANALIS` | `CAUTELAR COM ANALISE` |
| `LAUDO DE TRANSFEREN` | `LAUDO DE TRANSFERENCIA` |

Configuracao:

```env
TINY_SERVICO_ALIASES_JSON={"LAUDO DE VERIFICACA":"LAUDO DE VERIFICACAO","LAUDO CAUTELAR VERI":"LAUDO CAUTELAR VERIFICACAO","CAUTELAR COM ANALIS":"CAUTELAR COM ANALISE"}
```

O servico vai no historico do contas a receber. Nao e necessario cadastrar servico/produto no Tiny para este fluxo.

### 4. Categoria financeira

Se voces usam categoria no financeiro, preencha:

```env
TINY_CATEGORIA_ID=ID_DA_CATEGORIA
```

Para listar categorias:

```bash
python3 tiny_import.py --list-categorias
```

Se nao usam categoria, pode deixar vazio:

```env
TINY_CATEGORIA_ID=
```

### 5. Vencimento

Regra atual dos faturados:

```env
TINY_VENCIMENTO_TIPO=ultimo_dia_mes
```

Assim, um `FA` com data `13/04/2026` vence em `2026-04-30`.

Se em algum momento voces quiserem vencer por quantidade de dias, troque para:

```env
TINY_VENCIMENTO_TIPO=dias
TINY_VENCIMENTO_DIAS=7
```

Se quiser vencer na mesma data:

```env
TINY_VENCIMENTO_TIPO=mesma_data
```

## Checklist antes do primeiro envio real

1. `python3 tiny_import.py --check-env` precisa mostrar `OK - Token OAuth`.
2. Definir `TINY_FORMA_RECEBIMENTO_IDS_JSON` para `FA`.
3. Definir se os clientes serao cadastrados no Tiny ou mapeados por ID.
4. Rodar `python3 tiny_import.py --file 14_04_2026.xls`.
5. Conferir o arquivo `saida/payload_preview_*.json`.
6. So depois rodar `python3 tiny_import.py --send --archive`.

## Estado atual antes do envio

Autenticacao: OK.

Planilha: OK, 17 registros, total `R$ 3.830,00`.

Pendencias:

- Mapear ou cadastrar os clientes.
- Manter `AV` fora do envio automatico ao contas a receber.
- Confirmar se `FA` deve usar `802165201 - A faturar`.
- Confirmar se a categoria financeira e obrigatoria no seu processo.
