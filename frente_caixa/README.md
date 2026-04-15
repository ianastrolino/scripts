# Frente de Caixa

Prototipo local para conferir o caixa diario antes do envio ao Tiny.

Abra no navegador:

```text
/home/astro/projeto/scripts/frente_caixa/index.html
```

Fluxo atual:

1. Importar uma ou mais planilhas `.xls` exportadas pelo sistema antigo.
2. Separar `FA` para contas a receber.
3. Separar `AV` para fechamento do caixa.
4. Ajustar a forma real do `AV`: dinheiro, debito, credito ou Pix.
5. Conferir totais e pendencias.
6. Exportar a conferencia em JSON.

O painel reconhece no mesmo lote laudos cautelares/verificacao e vistoria de transferencia quando as colunas principais seguem o mesmo padrao: data, modelo, placa, cliente, servico, FP e preco.

O botao `Enviar FA para Tiny` ainda esta bloqueado no prototipo. O proximo passo e ligar esse botao ao backend Python que ja autentica e envia ao Tiny.
