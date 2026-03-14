# Diagnostico por Modulo

Este documento resume o estado atual dos modulos principais do app de conciliacao, com foco em objetivo, endpoints, fluxo de dados e riscos.

## integrations/interdata-import

Objetivo
- Importar arquivos Interdata (scan e upload), normalizar e persistir vendas para conciliacao.

Endpoints principais
- GET `admin/interdata/ping`
- GET `admin/interdata/files`
- POST `admin/interdata/import/scan`
- POST `admin/interdata/import/upload`
- GET `admin/interdata/import/progress` (SSE por uploadId)
- GET `admin/interdata/sales`
- POST `admin/interdata/sales/approve`
- POST `admin/interdata/sales/clear`
- POST `admin/interdata/reconciliation/run`

Fluxo de dados
- Arquivos entram via scan de pasta ou upload -> parser -> normalizacao -> `T_INTERDATA_SALES`.
- Registros invalidos/duplicados vao para tabelas de revisao.
- Ao concluir, pode disparar conciliacao automatica.

Tabelas/entidades principais
- `T_INTERDATA_SALES`, `T_INTERDATA_SALES_INVALID`, `T_INTERDATA_SALES_DUPLICATE`, `T_INTERDATA_FILES`.

Riscos e pontos a melhorar
- Variacoes de layout dos arquivos podem quebrar parser.
- Duplicidade depende de heuristicas fixas.
- Limpeza (clear) e reconciliacao removem dados; precisa de governanca.

## integrations/acquirer-import

Objetivo
- Importar vendas das adquirentes (Cielo/Sipag) via upload de arquivos e listar vendas com filtros.

Endpoints principais
- GET `admin/acquirer-import/ping`
- POST `admin/acquirer-import/upload`
- GET `admin/acquirer-import/sales`

Fluxo de dados
- Upload -> parsing -> normalizacao -> `T_CIELO_SALES` ou `T_SIPAG_SALES`.
- Listagem aplica filtros e ordenacao no banco.

Tabelas/entidades principais
- `T_CIELO_SALES`, `T_SIPAG_SALES`.

Riscos e pontos a melhorar
- Diferencas de layout/versao de arquivos podem exigir novos mapeamentos.
- Normalizacao de bandeira/tipo de pagamento ainda varia por fonte.
- Falta tracking de origem do arquivo por registro.

## modules/reconciliation

Objetivo
- Executar conciliacao automatica, listagem de conciliados e conciliacao manual.

Endpoints principais
- POST `admin/reconciliation/run`
- GET `admin/reconciliation/list`
- POST `admin/reconciliation/manual`

Fluxo de dados
- `ReconciliationService` percorre `T_INTERDATA_SALES` pendentes -> match cascade -> grava `T_RECONCILIATION` -> remove interdata e adquirente conciliados.
- Conciliacao manual valida conflitos, grava `T_RECONCILIATION` e remove registros originais.

Tabelas/entidades principais
- `T_RECONCILIATION`, `T_INTERDATA_SALES`, `T_CIELO_SALES`, `T_SIPAG_SALES`.

Riscos e pontos a melhorar
- Regras de match sao fixas e nao configuraveis por adquirente.
- Remocao de registros apos conciliacao reduz auditabilidade.
- Falta de campo estruturado para motivo/regra/score na conciliacao automatica.

## modules/jobs

Objetivo
- Executar conciliacao automatica periodica.

Endpoints principais
- N/A (cron interno).

Fluxo de dados
- Job horario chama reconciliacao se nao houver importacao em andamento.

Tabelas/entidades principais
- Mesmas da conciliacao.

Riscos e pontos a melhorar
- Pode conflitar com cargas longas se o lock nao for respeitado.
- Sem observabilidade detalhada (duracao, throughput).

## modules/sipag

Objetivo
- Importar transacoes Sipag via API (D-1).

Endpoints principais
- POST `admin/sipag/import`

Fluxo de dados
- Chama API Sipag com paginacao -> persiste payload bruto -> normaliza para `ACQ_TX`.

Tabelas/entidades principais
- `ACQ_RAW`, `ACQ_TX`.

Riscos e pontos a melhorar
- Dependencia de API externa e token.
- Duplicidade baseada em hash/poucos campos.

## auth e config

Objetivo
- Garantir acesso admin e configurar conexao Firebird.

Componentes principais
- `AdminGuard` valida `ADMIN_TOKEN` via header/query.
- `firebirdProvider` cria pool com variaveis `FB_*`.

Riscos e pontos a melhorar
- Autenticacao por token simples (sem roles, sem rotacao).
- Falta de configuracao de timeouts/retries no pool.
