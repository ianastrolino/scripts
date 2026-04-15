# Importacao diaria para Tiny/Olist

Este fluxo le o arquivo diario exportado pelo outro sistema, padroniza somente os campos importantes e, quando habilitado, cria lancamentos em Contas a Receber no Tiny/Olist pela API v3.

## Onde colocar o arquivo diario

Coloque o arquivo baixado todo dia em:

```bash
/home/astro/projeto/scripts/entrada
```

O script usa o arquivo mais recente dessa pasta quando voce nao informa um caminho especifico.

## Primeiro teste, sem enviar ao Tiny

```bash
python3 tiny_import.py --file 14_04_2026.xls
```

Isso cria:

- `saida/padronizado_*.csv`
- `saida/padronizado_*.json`
- `saida/payload_preview_*.json`
- `logs/resumo_importacao_*.json`

Por padrao o script roda em dry-run. Ele nao envia nada ao Tiny sem `--send`.

## Configuracao pelo .env

O arquivo principal de configuracao local e o `.env`:

```bash
cp .env.example .env
```

Depois preencha o `.env` com as credenciais e IDs corretos.
Esse arquivo esta no `.gitignore` porque vai guardar segredo da API.

Para um passo a passo bem detalhado de onde pegar cada valor, veja [GUIA_ENV.md](GUIA_ENV.md).
Para ver as regras de padronizacao antes do envio real, veja [COMPARATIVO_PADRONIZACAO_TINY.md](COMPARATIVO_PADRONIZACAO_TINY.md).
O prototipo do frente de caixa esta em [frente_caixa/index.html](frente_caixa/index.html).
Se estiver usando Live Server na raiz do projeto, abra `http://localhost:5500` ou `http://localhost:5500/frente_caixa/index.html`.

Para validar o `.env` sem mostrar segredos:

```bash
python3 tiny_import.py --check-env
```

Credenciais:

- `TINY_CLIENT_ID`: ID do aplicativo/integracao criado no Tiny.
- `TINY_CLIENT_SECRET`: chave secreta desse aplicativo.
- `TINY_REFRESH_TOKEN`: token usado para renovar o acesso.
- `TINY_ACCESS_TOKEN`: token temporario, se voce quiser testar sem refresh token.
- `TINY_REDIRECT_URI`: URL de redirecionamento usada na autorizacao OAuth.

IDs e regras:

- `TINY_CLIENTE_IDS_JSON`: mapa de nome do cliente para ID do contato no Tiny.
- `TINY_FORMA_RECEBIMENTO_IDS_JSON`: mapa do campo `FP` para ID da forma de recebimento.
- `TINY_CATEGORIA_ID`: ID da categoria financeira de receita, se voces usam categoria nos recebiveis.
- `TINY_CONTAS_RECEBER_FP_JSON`: quais FPs devem virar contas a receber. Por padrao, apenas `FA`.
- `TINY_VENCIMENTO_TIPO`: regra de vencimento. Para faturados, use `ultimo_dia_mes`.
- `TINY_VENCIMENTO_DIAS`: usado somente quando `TINY_VENCIMENTO_TIPO=dias`.

Exemplos:

```env
TINY_CLIENTE_IDS_JSON={"MARIN IMPORT":123,"EUROPAMOTORS":124}
TINY_FORMA_RECEBIMENTO_IDS_JSON={"FA":802165201}
TINY_CATEGORIA_ID=456
TINY_CONTAS_RECEBER_FP_JSON=["FA"]
TINY_VENCIMENTO_TIPO=ultimo_dia_mes
TINY_VENCIMENTO_DIAS=0
```

Mantenha os valores JSON em uma linha, como nos exemplos acima.

O arquivo exportado encurta alguns nomes de servico, por exemplo `CAUTELAR COM ANALIS`. A variavel `TINY_SERVICO_ALIASES_JSON` padroniza esses nomes antes de montar o historico do contas a receber.

Se quiser que o script crie contatos/clientes que nao existem, altere:

```env
TINY_AUTO_CREATE_CONTACTS=true
```

Use isso so depois de testar, porque cria cadastro real no ERP.

Cada lancamento no contas a receber vai com:

- `data`: data da planilha.
- `dataVencimento`: ultimo dia do mes para `FA`, salvo configuracao diferente.
- `valor`: preco.
- `contato.id`: cliente localizado no Tiny pelo nome.
- `formaRecebimento`: ID localizado a partir do campo `FP`, se configurado/encontrado.
- `historico`: servico, modelo, placa, FP e origem da importacao.

Regra atual: apenas `FA` e enviado ao contas a receber. `AV` fica para o painel/fechamento do caixa, porque pode ser dinheiro, cartao credito, cartao debito ou Pix.

## Autenticacao

A API v3 do Tiny/Olist usa OAuth 2. Em termos simples: `client_id` e `client_secret` identificam o aplicativo/integracao que voce cria dentro do Tiny, e o token autoriza esse aplicativo a mexer na sua conta do ERP.

O script carrega automaticamente o `.env` da pasta do projeto. Se quiser usar outro arquivo:

```bash
python3 tiny_import.py --env-file caminho/do/arquivo.env --file 14_04_2026.xls
```

Tambem da para trocar um codigo OAuth por tokens:

```bash
python3 tiny_import.py --exchange-code "CODIGO_RECEBIDO" --redirect-uri "SUA_REDIRECT_URI"
```

Os tokens ficam em `state/tiny_tokens.json`.

Depois de autenticar, estes comandos ajudam a descobrir IDs sem usar curl:

```bash
python3 tiny_import.py --list-clientes "MARIN IMPORT"
python3 tiny_import.py --list-formas-recebimento
python3 tiny_import.py --list-categorias
```

Para gerar a URL correta de autorizacao OAuth a partir do `.env`:

```bash
python3 tiny_import.py --auth-url
```

O jeito mais simples e deixar o script capturar o retorno sozinho:

```bash
python3 tiny_import.py --oauth-local
```

Ele fica aguardando em `http://localhost:8080/callback`, imprime a URL de autorizacao, e troca o `code` automaticamente quando voce autoriza no navegador.

## Enviar de verdade

Depois de conferir o CSV/JSON e configurar os IDs:

```bash
python3 tiny_import.py --file entrada/14_04_2026.xls --send
```

Para mover o arquivo para `processados/` depois de enviar sem falhas:

```bash
python3 tiny_import.py --send --archive
```

O script grava as chaves importadas em `state/imported.json` para evitar duplicidade.

## Agendamento diario

Exemplo de cron para rodar todos os dias as 19:00:

```cron
0 19 * * * cd /home/astro/projeto/scripts && /usr/bin/python3 tiny_import.py --send --archive >> logs/cron.log 2>&1
```

Antes de ativar o cron com `--send`, rode alguns dias em dry-run:

```cron
0 19 * * * cd /home/astro/projeto/scripts && /usr/bin/python3 tiny_import.py >> logs/cron.log 2>&1
```
