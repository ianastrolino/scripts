# Guia para preencher o `.env`

Este guia mostra onde pegar cada valor do arquivo `.env` para a importacao diaria de contas a receber no Tiny/Olist.

Para conferir o `.env` sem mostrar segredos, rode:

```bash
python3 tiny_import.py --check-env
```

## 1. Antes de comecar

Voce precisa entrar no Tiny/Olist com um usuario administrador.

A API v3 usa OAuth 2. Na pratica, isso significa:

- `client_id` e `client_secret`: identificam o aplicativo/integracao criado no Tiny.
- `code`: codigo temporario que aparece quando voce autoriza o aplicativo.
- `access_token`: chave temporaria para chamar a API.
- `refresh_token`: chave usada para renovar o acesso sem autorizar tudo de novo.

## 2. Criar o aplicativo no Tiny/Olist

No Tiny/Olist, procure a area de aplicativos/integracoes/API e crie um aplicativo para esta automacao.

Na criacao do aplicativo:

1. Informe um nome simples, por exemplo `Importacao contas a receber`.
2. Informe a URL de redirecionamento.
3. Marque apenas as permissoes necessarias.
4. Salve e copie as chaves de acesso.

Permissoes recomendadas:

- Contatos: consultar. Criar tambem se voce for usar `TINY_AUTO_CREATE_CONTACTS=true`.
- Contas a receber: criar.
- Formas de recebimento: consultar.
- Categorias de receita e despesa: consultar, se voce for usar `TINY_CATEGORIA_ID`.

## 3. Preencher credenciais basicas

No `.env`, preencha:

```env
TINY_CLIENT_ID=cole_aqui_o_client_id
TINY_CLIENT_SECRET=cole_aqui_o_client_secret
TINY_REDIRECT_URI=cole_aqui_a_mesma_url_configurada_no_app
```

De onde vem cada campo:

- `TINY_CLIENT_ID`: tela do aplicativo/integracao no Tiny.
- `TINY_CLIENT_SECRET`: tela do aplicativo/integracao no Tiny.
- `TINY_REDIRECT_URI`: a URL de redirecionamento que voce informou no aplicativo.

Importante: o valor de `TINY_REDIRECT_URI` precisa ser exatamente igual ao configurado no aplicativo.

## 4. Autorizar e gerar os tokens

Jeito recomendado: deixe o script capturar o retorno sozinho.

```bash
python3 tiny_import.py --oauth-local
```

Ele vai mostrar uma URL. Abra essa URL no navegador, autorize o app e volte para o terminal. O script troca o `code` por tokens automaticamente.

Se preferir fazer manualmente, gere a URL de autorizacao pelo script:

```bash
python3 tiny_import.py --auth-url
```

Abra a URL gerada no navegador.

Se preferir montar manualmente, use este formato:

```text
https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/auth?client_id=SEU_CLIENT_ID&redirect_uri=SUA_REDIRECT_URI&scope=openid&response_type=code
```

Substitua:

- `SEU_CLIENT_ID` pelo valor de `TINY_CLIENT_ID`.
- `SUA_REDIRECT_URI` pela URL de redirecionamento.

Se sua redirect URI for:

```text
http://localhost:8080/callback
```

Use na URL de autorizacao:

```text
http%3A%2F%2Flocalhost%3A8080%2Fcallback
```

Depois:

1. Abra a URL no navegador.
2. Faca login no Tiny/Olist com usuario administrador.
3. Autorize o aplicativo.
4. O navegador sera redirecionado para a URL configurada.
5. Copie o valor que aparece depois de `code=` na barra de endereco.

Exemplo de URL apos autorizar:

```text
http://localhost:8080/callback?code=abc123xyz
```

Neste exemplo, o codigo e:

```text
abc123xyz
```

Agora rode:

```bash
cd /home/astro/projeto/scripts
python3 tiny_import.py --exchange-code "COLE_O_CODE_AQUI"
```

Se voce ainda nao colocou `TINY_REDIRECT_URI` no `.env`, rode assim:

```bash
python3 tiny_import.py --exchange-code "COLE_O_CODE_AQUI" --redirect-uri "SUA_REDIRECT_URI"
```

O script salva os tokens em:

```text
state/tiny_tokens.json
```

Voce nao precisa preencher `TINY_ACCESS_TOKEN` manualmente quando usar esse metodo.

## 5. TINY_ACCESS_TOKEN

Campo no `.env`:

```env
TINY_ACCESS_TOKEN=
```

Uso recomendado: deixar vazio.

Esse token e temporario. Use somente para teste rapido se o Tiny/Olist te entregar um access token diretamente.

## 6. TINY_REFRESH_TOKEN

Campo no `.env`:

```env
TINY_REFRESH_TOKEN=
```

Uso recomendado: deixar vazio se voce usou `--exchange-code`, porque o script salva e renova em `state/tiny_tokens.json`.

Preencha manualmente apenas se voce ja tiver um refresh token valido.

## 7. TINY_CATEGORIA_ID

Campo no `.env`:

```env
TINY_CATEGORIA_ID=
```

O que e: ID da categoria financeira que vai aparecer no contas a receber.

Se voces nao usam categoria financeira nos recebiveis, deixe vazio.

Para listar categorias depois de autenticar:

```bash
python3 tiny_import.py --list-categorias
```

Copie o `id` da categoria desejada e preencha:

```env
TINY_CATEGORIA_ID=123456789
```

## 8. TINY_VENCIMENTO_DIAS

Campo no `.env`:

```env
TINY_VENCIMENTO_TIPO=ultimo_dia_mes
TINY_CONTAS_RECEBER_FP_JSON=["FA"]
```

Regra recomendada para este projeto:

- `FA`: enviar para contas a receber em aberto.
- Vencimento de `FA`: ultimo dia do mes.
- `AV`: nao enviar para contas a receber por padrao; tratar no painel/caixa.

`TINY_VENCIMENTO_DIAS` so e usado se `TINY_VENCIMENTO_TIPO=dias`.

```env
TINY_VENCIMENTO_DIAS=0
```

O que e: quantos dias depois da data da planilha o recebivel vence quando a regra for por dias.

Exemplos:

- `0`: vence na mesma data do servico.
- `1`: vence no dia seguinte.
- `7`: vence uma semana depois.

## 9. TINY_CLIENTE_IDS_JSON

Campo no `.env`:

```env
TINY_CLIENTE_IDS_JSON={}
```

O que e: mapa entre o nome do cliente na planilha e o ID do contato no Tiny.

Primeiro tente listar o cliente:

```bash
python3 tiny_import.py --list-clientes "MARIN IMPORT"
```

Exemplo de retorno:

```text
id   nome
123  MARIN IMPORT
```

Depois preencha:

```env
TINY_CLIENTE_IDS_JSON={"MARIN IMPORT":123}
```

Com varios clientes:

```env
TINY_CLIENTE_IDS_JSON={"MARIN IMPORT":123,"EUROPAMOTORS":124,"2B MOTORS":125}
```

Mantenha tudo em uma unica linha.

Observacao: o script tambem tenta encontrar o cliente pelo nome no Tiny. Esse mapa e mais importante quando o nome da planilha nao bate exatamente com o cadastro.

## 10. TINY_FORMA_RECEBIMENTO_IDS_JSON

Campo no `.env`:

```env
TINY_FORMA_RECEBIMENTO_IDS_JSON={}
```

O que e: mapa entre o campo `FP` da planilha e o ID da forma de recebimento no Tiny.

Na regra atual, apenas `FA` vai para contas a receber. `AV` fica para o painel/caixa, porque precisa ser separado entre dinheiro, cartao de credito, cartao de debito ou Pix.

Na sua planilha aparecem valores como:

- `AV`
- `FA`

Para listar as formas de recebimento:

```bash
python3 tiny_import.py --list-formas-recebimento
```

Exemplo de retorno:

```text
id   nome
789  A vista
790  Faturado
```

Para `FA`, preencha:

```env
TINY_FORMA_RECEBIMENTO_IDS_JSON={"FA":790}
```

## 11. TINY_AUTO_CREATE_CONTACTS

Campo no `.env`:

```env
TINY_AUTO_CREATE_CONTACTS=false
```

Recomendacao: deixe `false` no comeco.

Quando `false`, se o cliente nao existir no Tiny, o script para e avisa. Isso evita criar cliente duplicado com nome errado.

Depois que voce confiar no fluxo, pode usar:

```env
TINY_AUTO_CREATE_CONTACTS=true
```

## 12. TINY_REQUIRE_PAYMENT_MAPPING

Campo no `.env`:

```env
TINY_REQUIRE_PAYMENT_MAPPING=false
```

Recomendacao inicial: `false`.

Quando `true`, o script para se nao conseguir transformar `AV` ou `FA` em uma forma de recebimento do Tiny.

Use `true` quando voce quiser garantir que todo recebivel sempre tenha forma de recebimento preenchida.

## 13. TINY_INCLUDE_FORMA_RECEBIMENTO

Campo no `.env`:

```env
TINY_INCLUDE_FORMA_RECEBIMENTO=true
```

Se estiver `true`, o script envia `formaRecebimento` para o Tiny quando conseguir encontrar o ID.

Se estiver `false`, o script cria o contas a receber sem forma de recebimento.

## 14. TINY_DEFAULT_TIPO_PESSOA

Campo no `.env`:

```env
TINY_DEFAULT_TIPO_PESSOA=J
```

So e usado quando `TINY_AUTO_CREATE_CONTACTS=true`.

Valores comuns:

- `J`: pessoa juridica.
- `F`: pessoa fisica.

## 15. Aliases

Campos no `.env`:

```env
TINY_SERVICO_ALIASES_JSON={"LAUDO DE VERIFICACA":"LAUDO DE VERIFICACAO","LAUDO CAUTELAR VERI":"LAUDO CAUTELAR VERIFICACAO","CAUTELAR COM ANALIS":"CAUTELAR COM ANALISE"}
TINY_FP_ALIASES_JSON={"AV":"AV","FA":"FA"}
TINY_CLIENTE_ALIASES_JSON={}
```

O que fazem:

- `TINY_SERVICO_ALIASES_JSON`: corrige nomes truncados do sistema antigo.
- `TINY_FP_ALIASES_JSON`: padroniza forma de pagamento da planilha.
- `TINY_CLIENTE_ALIASES_JSON`: corrige nome de cliente antes de procurar no Tiny.

Exemplo de cliente:

```env
TINY_CLIENTE_ALIASES_JSON={"EUROPA MOTORS":"EUROPAMOTORS"}
```

## 16. Pastas

Normalmente deixe assim:

```env
IMPORT_INPUT_DIR=entrada
IMPORT_OUTPUT_DIR=saida
IMPORT_ARCHIVE_DIR=processados
IMPORT_STATE_DIR=state
IMPORT_LOGS_DIR=logs
```

O arquivo diario deve ir em:

```text
/home/astro/projeto/scripts/entrada
```

## 17. Conferir antes de enviar

Rode dry-run:

```bash
python3 tiny_import.py --file 14_04_2026.xls
```

Abra o arquivo gerado em `saida/payload_preview_*.json` e confira:

- cliente
- valor
- data de vencimento
- categoria
- forma de recebimento
- historico

## 18. Enviar de verdade

Depois de conferir:

```bash
python3 tiny_import.py --send --archive
```

O script grava o que ja foi enviado em:

```text
state/imported.json
```

Isso ajuda a evitar envio duplicado.
