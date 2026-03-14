# Card Conciliation (Sipag)

Backend NestJS para conciliar transacoes de cartao da Sipag usando Firebird (.FDB), preparado para evoluir com novas operadoras.

## Objetivo
- Buscar diariamente (D-1) o extrato de transacoes via API Sipag
- Salvar payload bruto para auditoria
- Normalizar transacoes em um formato padrao
- Preparar base para conciliacao com vendas internas
- Executar automaticamente via cron diario

## Stack
- Node.js 20+
- NestJS
- Firebird SQL (node-firebird)
- Axios (@nestjs/axios)
- Config (@nestjs/config)
- Scheduler (@nestjs/schedule)

## Estrutura do projeto
```
card-conciliation/
├─ src/
│  ├─ app.module.ts
│  ├─ main.ts
│  ├─ config/
│  │  └─ firebird.provider.ts
│  ├─ db/
│  │  └─ db.service.ts
│  ├─ modules/
│  │  ├─ sipag/
│  │  │  ├─ sipag.module.ts
│  │  │  ├─ sipag.service.ts
│  │  │  ├─ sipag.mapper.ts
│  │  │  └─ sipag.types.ts
│  │  ├─ sicoobPix/
│  │  │  ├─ sicoobPix.module.ts
│  │  │  ├─ sicoobPix.service.ts
│  │  │  ├─ sicoobPix.token.service.ts
│  │  │  ├─ sicoobPix.mapper.ts
│  │  │  ├─ sicoobPix.types.ts
│  │  │  └─ sicoobPix.controller.ts
│  │  ├─ reconciliation/
│  │  │  ├─ reconciliation.module.ts
│  │  │  └─ reconciliation.service.ts
│  │  └─ jobs/
│  │     ├─ jobs.module.ts
│  │     └─ dminus1.job.ts
│  └─ .env.example
├─ package.json
└─ README.md
```

## Configuracao
1) Copie `src/.env.example` para `.env` na raiz do projeto.
2) Preencha as variaveis conforme seu ambiente.

```
# Firebird
FB_HOST=127.0.0.1
FB_PORT=3050
FB_DATABASE=/opt/firebird/data/CONCILIACAO.FDB
FB_USER=SYSDBA
FB_PASSWORD=masterkey
FB_POOL_SIZE=10

# App
APP_CORS_ORIGIN=http://localhost:5173
ADMIN_TOKEN=troque_este_token
RECONCILIATION_WINDOW_MINUTES=120
MAINTENANCE_ENABLED=false

# Sipag API
SIPAG_BASE_URL=https://api.sipag.com.br
SIPAG_ENDPOINT_EXTRATO=/v1/transactions
SIPAG_TOKEN=SEU_TOKEN_AQUI

# Sicoob Pix (sandbox)
SICOOB_AUTH_URL=https://auth.sicoob.com.br/auth/realms/cooperado/protocol/openid-connect/token
SICOOB_PIX_BASE_URL=https://sandbox.sicoob.com.br/sicoob/sandbox/pix/api/v2
SICOOB_CLIENT_ID=SEU_CLIENT_ID
SICOOB_SCOPE=coloque_os_scopes_exatos_do_portal
SICOOB_GRANT_TYPE=client_credentials
SICOOB_TIMEOUT_MS=20000

# Cielo F360
CIELO_F360_BASE_URL=https://financas.f360.com.br
CIELO_F360_INTEGRATION_TOKEN=SEU_TOKEN_AQUI
CIELO_F360_ENABLED=false

# Cielo EDI (SFTP)
CIELO_EDI_MODE=local
CIELO_EDI_LOCAL_DIR=./data/cielo/edi
CIELO_EDI_ARCHIVE_DIR=./data/cielo/archive
CIELO_EDI_ERROR_DIR=./data/cielo/error
CIELO_EDI_FILE_GLOB=*.*
CIELO_SFTP_HOST=
CIELO_SFTP_PORT=22
CIELO_SFTP_USER=
CIELO_SFTP_PASSWORD=
CIELO_SFTP_PRIVATE_KEY_PATH=
CIELO_SFTP_REMOTE_DIR=/in
CIELO_SFTP_STRICT_HOSTKEY_CHECKING=true

# Interdata import
INTERDATA_ENABLED=true
INTERDATA_DROP_DIR=./data/interdata/inbox
INTERDATA_ARCHIVE_DIR=./data/interdata/archive
INTERDATA_ERROR_DIR=./data/interdata/error
INTERDATA_FILE_GLOB=*.xls*
INTERDATA_TZ=America/Porto_Velho
INTERDATA_SOURCE_NAME=INTERDATA
```

## Como rodar
```
npm install
npm run start:dev
```

## Como funciona a conciliacao D-1
- O job `Dminus1Job` roda diariamente as 06:30.
- Busca as transacoes do dia anterior (D-1) na Sipag.
- Salva o payload bruto em `ACQ_RAW` com hash SHA256.
- Normaliza as transacoes e salva em `ACQ_TX`.
- Registra o status da execucao em `JOB_RUNS`.

## Como funciona o Pix Sicoob (D-1)
- O job `SicoobPixJob` roda diariamente as 06:40.
- Busca recebimentos Pix do dia anterior (D-1).
- Salva o payload bruto em `PIX_RAW` com hash SHA256.
- Normaliza as transacoes e salva em `PIX_TX`.
- Registra o status da execucao em `JOB_RUNS`.

## Como testar o Pix Sicoob
```
curl http://localhost:3000/health/db
curl http://localhost:3000/admin/sicoob-pix/ping
curl -X POST \"http://localhost:3000/admin/sicoob-pix/import?date=2026-01-06\"
```

## Autenticacao admin
O app agora exige login com usuario e senha.
As credenciais e as sessoes ficam persistidas no Firebird.

Fluxo:
1) Se o banco ainda nao tiver usuarios, a primeira tela do frontend permite criar o usuario administrador inicial.
2) Depois disso, o acesso e feito por login e sessao via cookie HttpOnly.
3) Os endpoints em `/admin/*` aceitam a sessao autenticada. O `ADMIN_TOKEN` pode continuar existindo apenas como fallback tecnico para automacoes/cURL.

## Como testar Cielo F360
Ative a integracao com `CIELO_F360_ENABLED=true` antes de testar.
```
curl http://localhost:3000/admin/cielo-f360/ping
curl http://localhost:3000/admin/cielo-f360/contas
curl -X POST http://localhost:3000/admin/cielo-f360/extrato -H \"Content-Type: application/json\" -d '{\"DataInicio\":\"2024-01-01\",\"DataFim\":\"2024-01-31\"}'
curl \"http://localhost:3000/admin/cielo-f360/parcelas-cartoes?inicio=2024-01-01&fim=2024-01-31&pagina=1\"
curl -X POST http://localhost:3000/admin/cielo-f360/relatorios/conciliacao-cartoes -H \"Content-Type: application/json\" -d '{\"DataInicio\":\"2024-01-01\",\"DataFim\":\"2024-01-31\"}'
```

## Cielo EDI (SFTP) - Operacao local
Em producao, o fluxo e local (o SFTP da Cielo entrega em um diretorio local).
1) Crie os diretorios locais (padrao DEV):
```
mkdir -p ./data/cielo/edi ./data/cielo/archive ./data/cielo/error
```
2) Coloque um arquivo EDI de teste em `./data/cielo/edi`.
3) Rode o scan:
```
curl -X POST http://localhost:3000/admin/cielo-edi/scan
```

Teste manual rapido (dummy):
```
echo \"CIELO03|TESTE\" > ./data/cielo/edi/CIELO03_TESTE_001.txt
curl -X POST http://localhost:3000/admin/cielo-edi/scan
ls -la ./data/cielo/archive
```

## Cielo EDI (SFTP) - DEV (sync remoto)
Com `CIELO_EDI_MODE=sftp`, o endpoint de sync baixa arquivos do SFTP remoto.
```
curl http://localhost:3000/admin/cielo-edi/ping
curl http://localhost:3000/admin/cielo-edi/files
curl -X POST http://localhost:3000/admin/cielo-edi/sync
```

## Importacao Interdata (Excel)
1) Crie os diretorios locais:
```
mkdir -p ./data/interdata/inbox ./data/interdata/archive ./data/interdata/error
```
2) Envie o arquivo via upload ou coloque direto em `./data/interdata/inbox`.

### Testes manuais
```
curl http://localhost:3000/admin/interdata/ping
curl http://localhost:3000/admin/interdata/files
curl -X POST http://localhost:3000/admin/interdata/import/scan
curl -X POST http://localhost:3000/admin/interdata/import/upload -F \"file=@/caminho/arquivo.xlsx\"
curl \"http://localhost:3000/admin/interdata/sales?dateFrom=2024-01-01&dateTo=2024-01-31&page=1&limit=50\"
```

## Conciliacao
Crie a tabela usando `docs/DDL_RECONCILIATION.sql`.

Fluxo sugerido:
1) Importar vendas Interdata.
2) Importar CSVs de adquirentes (Cielo/Sipag).
3) A conciliacao roda automaticamente apos cada importacao Interdata e a cada 1 hora (se houver vendas pendentes).

```
curl -X POST \"http://localhost:3000/admin/reconciliation/run?dateFrom=2024-01-01&dateTo=2024-01-31&acquirer=CIELO&limit=100\" -H \"Authorization: Bearer <ADMIN_TOKEN>\"
```

### Verificar SALE_NO (apos importacao)
```
SELECT SALE_NO, SALE_DATETIME, GROSS_AMOUNT FROM T_INTERDATA_SALES ORDER BY SALE_DATETIME DESC;
```

## Como adicionar novas operadoras
1) Crie um modulo em `src/modules/<operadora>` seguindo o padrao da Sipag.
2) Implemente o mapper para normalizacao no formato padrao.
3) Adicione o job diario ou agende conforme necessidade.
4) Atualize o README com a documentacao da nova operadora.

## Scripts SQL (documentacao)
> Nao executar automaticamente. Ajuste os tipos conforme sua instalacao Firebird.

### SALES
```sql
CREATE TABLE SALES (
  ID BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  SALE_DATE DATE,
  ORDER_ID VARCHAR(60),
  NSU VARCHAR(40),
  TID VARCHAR(40),
  AUTH_CODE VARCHAR(20),
  GROSS_AMOUNT NUMERIC(15, 2),
  NET_AMOUNT NUMERIC(15, 2),
  BRAND VARCHAR(20),
  INSTALLMENTS SMALLINT,
  CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### ACQ_RAW
```sql
CREATE TABLE ACQ_RAW (
  ID BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  PROVIDER VARCHAR(20) NOT NULL,
  REF_DATE DATE NOT NULL,
  PAYLOAD_HASH VARCHAR(64) NOT NULL,
  PAYLOAD_JSON BLOB SUB_TYPE TEXT,
  CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### ACQ_TX
```sql
CREATE TABLE ACQ_TX (
  ID BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  PROVIDER VARCHAR(20) NOT NULL,
  REF_DATE DATE NOT NULL,
  EXT_ID VARCHAR(80),
  NSU VARCHAR(40),
  AUTH_CODE VARCHAR(20),
  TID VARCHAR(40),
  GROSS_AMOUNT NUMERIC(15, 2),
  NET_AMOUNT NUMERIC(15, 2),
  FEE_AMOUNT NUMERIC(15, 2),
  BRAND VARCHAR(20),
  INSTALLMENTS SMALLINT,
  STATUS_ACQ VARCHAR(20),
  CAPTURED_AT TIMESTAMP,
  SETTLEMENT_DATE DATE,
  CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### RECON_RESULTS
```sql
CREATE TABLE RECON_RESULTS (
  ID BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  REF_DATE DATE NOT NULL,
  PROVIDER VARCHAR(20) NOT NULL,
  SALES_ID BIGINT,
  ACQ_TX_ID BIGINT,
  STATUS VARCHAR(20),
  NOTES VARCHAR(255),
  CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### JOB_RUNS
```sql
CREATE TABLE JOB_RUNS (
  ID BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  JOB_NAME VARCHAR(60) NOT NULL,
  REF_DATE DATE NOT NULL,
  STATUS VARCHAR(20) NOT NULL,
  STARTED_AT TIMESTAMP,
  FINISHED_AT TIMESTAMP,
  ERROR_MESSAGE VARCHAR(500)
);
```

### T_EDI_FILES
```sql
CREATE TABLE T_EDI_FILES (
  ID BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  FILENAME VARCHAR(255) NOT NULL,
  SHA256 CHAR(64),
  TYPE VARCHAR(10),
  SIZE BIGINT,
  MTIME TIMESTAMP,
  STATUS VARCHAR(20),
  PROCESSED_AT TIMESTAMP,
  ERROR_MESSAGE VARCHAR(500)
);
```

### T_INTERDATA_FILES
```sql
CREATE TABLE T_INTERDATA_FILES (
  ID BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  FILENAME VARCHAR(255) NOT NULL,
  SHA256 CHAR(64),
  SIZE BIGINT,
  MTIME TIMESTAMP,
  STATUS VARCHAR(20),
  PROCESSED_AT TIMESTAMP,
  ERROR_MESSAGE VARCHAR(500)
);
```

### T_INTERDATA_SALES
```sql
CREATE TABLE T_INTERDATA_SALES (
  ID BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  SALE_NO VARCHAR(50) NOT NULL,
  SOURCE VARCHAR(50) DEFAULT 'INTERDATA',
  SALE_DATETIME TIMESTAMP,
  AUTH_NSU VARCHAR(60),
  CARD_BRAND_RAW VARCHAR(120),
  PAYMENT_TYPE VARCHAR(20),
  CARD_MODE VARCHAR(20),
  INSTALLMENTS INTEGER,
  GROSS_AMOUNT NUMERIC(15, 2),
  FEES_AMOUNT NUMERIC(15, 2),
  NET_AMOUNT NUMERIC(15, 2),
  STATUS_RAW VARCHAR(50),
  IS_CANCELLED SMALLINT,
  ROW_HASH CHAR(64),
  CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Atualizacao incremental (SALE_NO)
```sql
-- Verificar se a coluna existe
SELECT RDB$FIELD_NAME FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME = 'T_INTERDATA_SALES';

-- Se nao existir:
ALTER TABLE T_INTERDATA_SALES ADD SALE_NO VARCHAR(50);

-- Backfill opcional (exemplo usando AUTH_NSU ou ROW_HASH)
UPDATE T_INTERDATA_SALES SET SALE_NO = AUTH_NSU WHERE SALE_NO IS NULL AND AUTH_NSU IS NOT NULL;
UPDATE T_INTERDATA_SALES SET SALE_NO = ROW_HASH WHERE SALE_NO IS NULL;

-- Tentar tornar NOT NULL (se permitido)
-- ALTER TABLE T_INTERDATA_SALES ALTER SALE_NO SET NOT NULL;

-- Unico por numero da venda
CREATE UNIQUE INDEX UX_INTERDATA_SALES_SALE_NO ON T_INTERDATA_SALES (SALE_NO);
```

## Pix Sicoob (DDL)
Arquivo: `docs/DDL_PIX_SICOOB.sql`
