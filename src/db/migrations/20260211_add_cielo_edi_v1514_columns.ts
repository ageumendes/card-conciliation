import * as Firebird from 'node-firebird';

type ColumnDef = { name: string; type: string };

const columns: ColumnDef[] = [
  { name: 'E_RECORD_TYPE', type: 'CHAR(1)' },
  { name: 'E_SUBMIT_ESTABLISHMENT', type: 'VARCHAR(10)' },
  { name: 'E_SETTLEMENT_BRAND', type: 'VARCHAR(3)' },
  { name: 'E_SETTLEMENT_TYPE', type: 'VARCHAR(3)' },
  { name: 'E_INSTALLMENT_NO', type: 'SMALLINT' },
  { name: 'E_INSTALLMENT_TOTAL', type: 'SMALLINT' },
  { name: 'E_AUTH_CODE', type: 'VARCHAR(6)' },
  { name: 'E_ENTRY_TYPE_CODE', type: 'VARCHAR(2)' },
  { name: 'E_UR_KEY', type: 'VARCHAR(100)' },
  { name: 'E_RECEIVED_TRANS_CODE', type: 'VARCHAR(22)' },
  { name: 'E_ADJUSTMENT_CODE', type: 'VARCHAR(4)' },
  { name: 'E_PAYMENT_METHOD_CODE', type: 'VARCHAR(3)' },
  { name: 'E_IND_PROMO', type: 'CHAR(1)' },
  { name: 'E_IND_DCC', type: 'CHAR(1)' },
  { name: 'E_IND_MIN_COMMISSION', type: 'CHAR(1)' },
  { name: 'E_IND_RA_TC', type: 'CHAR(1)' },
  { name: 'E_IND_ZERO_FEE', type: 'CHAR(1)' },
  { name: 'E_FLAG_REJECTED', type: 'CHAR(1)' },
  { name: 'E_IND_LATE_SALE', type: 'CHAR(1)' },
  { name: 'E_CARD_BIN', type: 'VARCHAR(6)' },
  { name: 'E_CARD_LAST4', type: 'VARCHAR(4)' },
  { name: 'E_NSU_DOC', type: 'VARCHAR(6)' },
  { name: 'E_INVOICE_NO', type: 'VARCHAR(10)' },
  { name: 'E_TID', type: 'VARCHAR(20)' },
  { name: 'E_ORDER_CODE', type: 'VARCHAR(20)' },
  { name: 'E_MDR_RATE', type: 'NUMERIC(9, 4)' },
  { name: 'E_RA_RATE', type: 'NUMERIC(9, 4)' },
  { name: 'E_SALE_RATE', type: 'NUMERIC(9, 4)' },
  { name: 'E_TOTAL_AMOUNT_SIGN', type: 'CHAR(1)' },
  { name: 'E_TOTAL_AMOUNT', type: 'NUMERIC(15, 2)' },
  { name: 'E_GROSS_AMOUNT_SIGN', type: 'CHAR(1)' },
  { name: 'E_GROSS_AMOUNT', type: 'NUMERIC(15, 2)' },
  { name: 'E_NET_AMOUNT_SIGN', type: 'CHAR(1)' },
  { name: 'E_NET_AMOUNT', type: 'NUMERIC(15, 2)' },
  { name: 'E_COMMISSION_SIGN', type: 'CHAR(1)' },
  { name: 'E_COMMISSION_AMOUNT', type: 'NUMERIC(15, 2)' },
  { name: 'E_MIN_COMMISSION_SIGN', type: 'CHAR(1)' },
  { name: 'E_MIN_COMMISSION_AMOUNT', type: 'NUMERIC(15, 2)' },
  { name: 'E_ENTRY_SIGN', type: 'CHAR(1)' },
  { name: 'E_ENTRY_AMOUNT', type: 'NUMERIC(15, 2)' },
  { name: 'E_MDR_FEE_SIGN', type: 'CHAR(1)' },
  { name: 'E_MDR_FEE_AMOUNT', type: 'NUMERIC(15, 2)' },
  { name: 'E_FAST_RECEIVE_SIGN', type: 'CHAR(1)' },
  { name: 'E_FAST_RECEIVE_AMOUNT', type: 'NUMERIC(15, 2)' },
  { name: 'E_CASHOUT_SIGN', type: 'CHAR(1)' },
  { name: 'E_CASHOUT_AMOUNT', type: 'NUMERIC(15, 2)' },
  { name: 'E_SHIPMENT_FEE_SIGN', type: 'CHAR(1)' },
  { name: 'E_SHIPMENT_FEE_AMOUNT', type: 'NUMERIC(15, 2)' },
  { name: 'E_PENDING_SIGN', type: 'CHAR(1)' },
  { name: 'E_PENDING_AMOUNT', type: 'NUMERIC(15, 2)' },
  { name: 'E_DEBT_TOTAL_SIGN', type: 'CHAR(1)' },
  { name: 'E_DEBT_TOTAL_AMOUNT', type: 'NUMERIC(15, 2)' },
  { name: 'E_CHARGED_SIGN', type: 'CHAR(1)' },
  { name: 'E_CHARGED_AMOUNT', type: 'NUMERIC(15, 2)' },
  { name: 'E_ADMIN_FEE_SIGN', type: 'CHAR(1)' },
  { name: 'E_ADMIN_FEE_AMOUNT', type: 'NUMERIC(15, 2)' },
  { name: 'E_PROMO_SIGN', type: 'CHAR(1)' },
  { name: 'E_PROMO_AMOUNT', type: 'NUMERIC(15, 2)' },
  { name: 'E_DCC_SIGN', type: 'CHAR(1)' },
  { name: 'E_DCC_AMOUNT', type: 'NUMERIC(15, 2)' },
  { name: 'E_TIME_HHMMSS', type: 'VARCHAR(6)' },
  { name: 'E_CARD_GROUP', type: 'VARCHAR(2)' },
  { name: 'E_RECEIVER_DOCUMENT', type: 'VARCHAR(14)' },
  { name: 'E_AUTH_BRAND', type: 'VARCHAR(3)' },
  { name: 'E_SALE_UNIQUE_CODE', type: 'VARCHAR(15)' },
  { name: 'E_SALE_ORIGINAL_CODE', type: 'VARCHAR(15)' },
  { name: 'E_NEGOTIATION_EFFECT_ID', type: 'VARCHAR(15)' },
  { name: 'E_SALES_CHANNEL', type: 'VARCHAR(3)' },
  { name: 'E_LOGICAL_TERMINAL_NO', type: 'VARCHAR(8)' },
  { name: 'E_ORIGINAL_ENTRY_TYPE', type: 'VARCHAR(2)' },
  { name: 'E_TRANSACTION_TYPE', type: 'VARCHAR(3)' },
  { name: 'E_CIELO_USAGE_1', type: 'VARCHAR(4)' },
  { name: 'E_PRICING_MODEL_CODE', type: 'VARCHAR(5)' },
  { name: 'E_AUTH_DATE', type: 'DATE' },
  { name: 'E_CAPTURE_DATE', type: 'DATE' },
  { name: 'E_ENTRY_DATE', type: 'DATE' },
  { name: 'E_ORIGINAL_ENTRY_DATE', type: 'DATE' },
  { name: 'E_BATCH_NO', type: 'VARCHAR(7)' },
  { name: 'E_PROCESSED_TRANSACTION_NO', type: 'VARCHAR(22)' },
  { name: 'E_REJECT_REASON_CODE', type: 'VARCHAR(3)' },
  { name: 'E_SETTLEMENT_BLOCK', type: 'VARCHAR(22)' },
  { name: 'E_FLAG_CLIENT_INSTALLMENT', type: 'CHAR(1)' },
  { name: 'E_BANK_NO', type: 'VARCHAR(4)' },
  { name: 'E_AGENCY_NO', type: 'VARCHAR(5)' },
  { name: 'E_ACCOUNT_NO', type: 'VARCHAR(20)' },
  { name: 'E_ACCOUNT_DV', type: 'CHAR(1)' },
  { name: 'E_ARN', type: 'VARCHAR(23)' },
  { name: 'E_FLAG_RECEIVABLES_NEG', type: 'CHAR(1)' },
  { name: 'E_CAPTURE_TYPE', type: 'VARCHAR(2)' },
  { name: 'E_NEGOTIATOR_DOCUMENT', type: 'VARCHAR(14)' },
  { name: 'E_CIELO_USAGE_2', type: 'VARCHAR(38)' },
  { name: 'E_FILE_HEADER_DATE', type: 'DATE' },
  { name: 'E_RAW_LINE', type: 'VARCHAR(760)' },
  { name: 'PIX_ID', type: 'VARCHAR(40)' },
  { name: 'TX_ID', type: 'VARCHAR(36)' },
  { name: 'PIX_PAYMENT_ID', type: 'VARCHAR(36)' },
];

const options = {
  host: process.env.FB_HOST ?? '127.0.0.1',
  port: Number(process.env.FB_PORT ?? 3050),
  database: process.env.FB_DATABASE,
  user: process.env.FB_USER,
  password: process.env.FB_PASSWORD,
};

if (!options.database || !options.user || !options.password) {
  console.error('FB_DATABASE/FB_USER/FB_PASSWORD precisam estar definidos.');
  process.exit(1);
}

const attach = () =>
  new Promise<Firebird.Database>((resolve, reject) => {
    Firebird.attach(options, (err, db) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(db);
    });
  });

const query = (db: Firebird.Database, sql: string, params: unknown[] = []) =>
  new Promise<unknown[]>((resolve, reject) => {
    db.query(sql, params, (err, result) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(result as unknown[]);
    });
  });

const execute = (db: Firebird.Database, sql: string, params: unknown[] = []) =>
  new Promise<void>((resolve, reject) => {
    db.query(sql, params, (err) => {
      if (err) {
        reject(err);
        return;
      }
      resolve();
    });
  });

const columnExists = async (db: Firebird.Database, name: string) => {
  const rows = await query(
    db,
    'SELECT 1 FROM RDB$RELATION_FIELDS WHERE TRIM(RDB$RELATION_NAME) = ? AND TRIM(RDB$FIELD_NAME) = ?',
    ['T_CIELO_SALES', name],
  );
  return rows.length > 0;
};

const run = async () => {
  const db = await attach();
  try {
    for (const column of columns) {
      const exists = await columnExists(db, column.name);
      if (exists) {
        console.log(`OK: ${column.name} já existe`);
        continue;
      }
      await execute(db, `ALTER TABLE T_CIELO_SALES ADD ${column.name} ${column.type}`);
      console.log(`ADD: ${column.name} ${column.type}`);
    }
  } finally {
    db.detach();
  }
};

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
