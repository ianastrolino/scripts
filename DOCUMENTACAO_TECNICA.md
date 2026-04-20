# Documentação Técnica: Frente de Caixa & Integração Tiny / Astro Vistorias

Este documento fornece as diretrizes arquiteturais, o funcionamento interno e as premissas técnicas do sistema desenvolvido para gerenciar frentes de caixa e integrações sistêmicas (Tiny/Olist) de franquias de vistoria veicular.

---

## 1. Visão Geral da Arquitetura

O sistema adota uma arquitetura monolítica separada entre um backend robusto em Python (Flask) e um frontend construído de forma nativa (Vanilla JS, HTML5, CSS3) que dispensa empacotadores, o que confere respostas extremamente rápidas (microssegundos) e simplicidade de manutenção. 

O projeto tem um viés fortemente *multiplataforma* (Multi-Tenant nativo) projetado para gerenciar N (incontáveis) franquias de vistorias a partir de uma mesma imagem de container hospedada no Railway.

### Principais Componentes:
1. **API Web & Servidor Base (`server.py`)**: Gateway central que expõe endpoints de dados e serve o frontend. Gerencia sessões, segurança e fluxo de autorização OAuth2 com o ERP.
2. **Motor de Importação Tiny (`tiny_import.py`)**: Lógica complexa responsável por sanitizar registros, deduzir formas de pagamento, corrigir nomenclaturas originadas de sistemas arcaicos e se integrar via API v3 com o ERP Tiny/Olist. Controles de *Rate-Limiting* e resiliência integrados.
3. **Database Local (`caixa_db.py`)**: Armazenamento dos dados transacionais locais de caixa do dia e divergências, construído com SQLite e Fallbacks em JSON.
4. **Clientes Web (`frente_caixa/`)**: Conjunto poderoso de aplicações Vanilla JS (Caixa, Fechamento e Dashboard Master).

---

## 2. Tecnologias e Bibliotecas

- **Backend / Linguagem**: `Python 3.10+`
- **Framework Web**: `Flask` (com `Gunicorn` em produção)
- **Frontend**: Nativo (Vanilla HTML, CSS, JavaScript) sem framework react/vue para reduzir overhead.
- **Banco de Dados**: SQLite embarcado (dados transacionais intradiários).
- **Deploy e Hospedagem**: [Railway.app](https://railway.app), usando um arquivo padrão `Dockerfile` / `Procfile`.

---

## 3. Topologia Multi-Unidade (Multi-Tenant)

A escalabilidade por unidade (Moema, Mooca, Barueri, etc) funciona isoladamente em uma mesma máquina por conta de variáveis dinâmicas de configuração atreladas ao Railway.

- **`USERS_CONFIG`**: Um JSON serializado em variável de ambiente que contém usuários, senhas criadas sob *hash* de seguranças altíssimo (`pbkdf2_hmac`), as lógicas de cargos (`master: true/false`, `gerencial: true/false`) e associação direta com a UUID da referida unidade.
- **`UNITS_CONFIG`**: Controle global por unidade. Aqui se preenche os Tokens da API do Tiny (`client_id`, `client_secret` etc.), o PIN Master local da franquia (para exclusão de lançamentos no caixa) e IDs de mapeamento das configurações específicas daquele CNPJ no ERP.

O isolamento é provido via rotas no formato `/u/{nome_unidade}/...`.

---

## 4. O Fluxo de Negócio

### 4.1. PDV e Caixa Diário (`caixa2.html` / `caixa.js`)
Aplicação usada na ponta, pelos vistoriadores ou fiscais administrativos.
- A aplicação colhe dados como `Placa`, `Cliente` e a `Forma de Pagamento (À vista ou Faturado)`.
- Se o pagamento for feito incorretamente, a exclusão da linha demanda permissão de superior usando o **PIN Master** originário da Unidade Configurada.
- Mantém somadores em *Real-Time*, auxiliando no recolhimento do final de expedientes.

### 4.2. Fechamento de Caixa (`fechamento.html` / `app.js`)
Lógica administrativa crítica; uma auditoria cruzada (*Cross-Check Auditing*).
- O funcionário do escritório faz o upload da tabela exportada por um sistema legado de vistorias (.xls).
- O Frontend faz parsing local sem onerar os servidores. As linhas convertidas cruzam as informações com a base de dados batizada pelo PDV.
- Caso haja divergências de preço (`R$ 100` registrado no APP vs `R$ 150` no Excel), a linha não vai para a sincronização Tiny até que seja ratificada manualmente.
- Elementos sem correspondência entre a tabela e o Caixa recebem *flags* de pendência. Lançamentos "À vista (AV)" sempre aparecerão, inclusive sem planilha, devido a uma flexibilidade implementada posteriormente.

### 4.3. Interface de Sincronização Tiny ERP
Após a tela de "Fechamento" assegurar a consistência:
- Apenas lançamentos `FA` (Faturados / A receber) são transportados até as contas do Tiny ERP para efetivação contábil com vencimentos automáticos ao final do ciclo do mês decorrente.
- Se a inteligência artificial embarcada não compreender a forma de pagamento do ERP, o cliente não enviar o CPF ou a API rejeitar os dados, a integração devolve os erros diretamente na interface antes do dano permanente acontecer no Tiny.

---

## 5. Deploy / Infraestrutura Mínima no Railway

O funcionamento adequado no ambiente cloud prescinde de:

1. **Repositório Git conectado ao projeto Railway.**
2. **`DATA_DIR` persistente enviando para `/data` (Railway Volumes).** Isso garante que os bancos SQLite para a sessão daquele dia e a renovação via `OAUTH` (os refreshes tokens do Tiny) não sejam extirpados durante o processo periódico de auto-reboot do Container pelo provedor.
3. Criação dos arquivos `.env` baseados em `GUIA_ENV.md` nas *Variables* do Railway.

## 6. Padronizações de Código (Regras da Base)

Para manter a integridade, é preconizado:
- Não utilizar lógicas bloqueantes no JS (dar preferência ao padrão arquitetural `async/await` com Promises nativas).
- Lógicas de visual/estética não devem usar *Tailwind* sem motivo grave; focar prioritariamente na manutenção por classes limpas via `.css` central com foco em UI imersiva.
- Adições API RESTful no `server.py` devem passar pelo modelo de segregação `@unit_access_required` + validação do `@csrf_required` caso modifique o dado central.
