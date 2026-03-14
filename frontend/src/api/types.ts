export interface ApiListResponse<T> {
  ok: boolean;
  data: T[];
}

export interface ApiFilesResponse {
  ok: boolean;
  files: { filename: string; size: number; mtimeMs: number }[];
}

export interface ApiPingResponse {
  ok: boolean;
  [key: string]: unknown;
}

export interface InterdataSale {
  ID?: number;
  SOURCE?: string;
  SALE_NO?: string;
  SALE_DATETIME?: string;
  CREATED_AT?: string;
  AUTH_NSU?: string;
  CARD_BRAND_RAW?: string;
  PAYMENT_TYPE?: string;
  CARD_MODE?: string;
  INSTALLMENTS?: number;
  GROSS_AMOUNT?: number;
  FEES_AMOUNT?: number;
  NET_AMOUNT?: number;
  STATUS_RAW?: string;
  STATUS?: string;
  IS_CANCELLED?: number;
  ROW_HASH?: string;
}

export interface InterdataSalesResponse {
  ok: boolean;
  data: InterdataSale[];
}

export interface CieloSale {
  ID?: number;
  SALE_DATETIME?: string;
  ESTABLISHMENT_NO?: string;
  PAYMENT_METHOD?: string;
  BRAND?: string;
  GROSS_AMOUNT?: number;
  FEE_AMOUNT?: number;
  NET_AMOUNT?: number;
  STATUS?: string;
  ENTRY_TYPE?: string;
  REASON?: string;
  ENTRY_DATE?: string;
  SETTLEMENT_DATE?: string;
  AUTH_CODE?: string;
  NSU_DOC?: string;
  SALE_CODE?: string;
  TID?: string;
  CARD_ORIGIN?: string;
  PIX_ID?: string;
  TX_ID?: string;
  PIX_PAYMENT_ID?: string;
  CARD_NUMBER?: string;
  ORDER_NUMBER?: string;
  INVOICE_NUMBER?: string;
  BATCH_NUMBER?: string;
  SALES_CHANNEL?: string;
  MODALITY?: string;
  CAPTURE_TYPE?: string;
  MACHINE_NUMBER?: string;
  TOTAL_FEES?: number;
  MDR_RATE?: number;
  TERM_RATE?: number;
  MDR_AMOUNT?: number;
  TERM_AMOUNT?: number;
  CASH_AMOUNT?: number;
  CHANGE_AMOUNT?: number;
  ORIGIN_VALUE?: string;
  ORIGIN_DOCUMENT?: string;
  ORIGIN_INSTITUTION?: string;
  DEST_VALUE?: string;
  DEST_DOCUMENT?: string;
  DEST_INSTITUTION?: string;
  ROW_HASH?: string;
  CREATED_AT?: string;
}

export interface SipagSale {
  ID?: number;
  ESTABLISHMENT_NO?: string;
  SALE_DATETIME?: string;
  TRANSACTION_NO?: string;
  SALE_ID?: string;
  BRAND?: string;
  PAYMENT_METHOD?: string;
  PLAN_DESC?: string;
  INSTALLMENT_NO?: number;
  INSTALLMENT_TOTAL?: number;
  AUTH_NO?: string;
  CARD_TYPE?: string;
  CARD_NUMBER?: string;
  TERMINAL_NO?: string;
  CAPTURE_TYPE?: string;
  CREDIT_DEBIT_IND?: string;
  CANCEL_IND?: string;
  SUMMARY_NO?: string;
  SETTLEMENT_DATE?: string;
  YOUR_NUMBER?: string;
  PAYMENT_ORDER_NO?: string;
  STATUS?: string;
  GROSS_AMOUNT?: number;
  FEE_AMOUNT?: number;
  NET_AMOUNT?: number;
  PLAN_TOTAL?: number;
  ROW_HASH?: string;
  CREATED_AT?: string;
}

export interface SicrediSale {
  ID?: number;
  SALE_DATETIME?: string;
  AUTH_CODE?: string;
  ESTABLISHMENT_CODE?: string;
  ESTABLISHMENT_NAME?: string;
  SALE_RECEIPT?: string;
  ORDER_NO?: string;
  SALES_CHANNEL?: string;
  TERMINAL_NO?: string;
  PRODUCT?: string;
  CARD_TYPE?: string;
  BRAND?: string;
  STATUS?: string;
  GROSS_AMOUNT?: number;
  MDR_AMOUNT?: number;
  NET_AMOUNT?: number;
  ORDER_ID_DESC?: string;
  CARD_NUMBER?: string;
  PREPAID?: string;
  EXPECTED_PAY_DATE?: string;
  PAY_STATUS?: string;
  PAY_DATE?: string;
  PAYMENT_CODE?: string;
  CARD_REF_CODE?: string;
  CARD_ORIGIN?: string;
  ROW_HASH?: string;
  CREATED_AT?: string;
}

export interface AcquirerSalesResponse<T> {
  ok: boolean;
  data: T[];
}

export interface AcquirerUnifiedSale {
  acquirer: 'CIELO' | 'SIPAG' | 'SICREDI';
  id: number;
  saleDatetime: string;
  grossAmount: number;
  mdrAmount: number | null;
  netAmount: number | null;
  authCode: string | null;
  nsu: string | null;
  terminal: string | null;
  pdv: string | null;
  brand: string | null;
  status: string | null;
  raw?: Record<string, unknown>;
}

export interface ReconciliationRecord {
  ID?: number;
  INTERDATA_ID?: number;
  ACQUIRER_ID?: number;
  ACQUIRER?: string;
  ACQ_PROVIDER?: string;
  SOURCE?: string;
  REASON?: string;
  NOTES?: string;
  SALE_NO?: string;
  AUTH_NSU?: string;
  ACQ_AUTH_CODE?: string;
  ACQ_NSU?: string;
  SALE_DATETIME?: string;
  ACQ_SALE_DATETIME?: string;
  GROSS_AMOUNT?: number;
  ACQ_GROSS_AMOUNT?: number;
  NET_AMOUNT?: number;
  ACQ_NET_AMOUNT?: number;
  BRAND?: string;
  ACQ_BRAND?: string;
  STATUS?: string;
  MATCH_TYPE?: string;
  MATCH_SCORE?: number;
  AMOUNT_DIFF?: number;
  MATCH_LAYER?: number;
  MATCH_CONFIDENCE?: number;
  MATCH_REASON?: string;
  MATCHED_AT?: string;
  CREATED_AT?: string;
}

export interface ReconciliationMatch {
  ID?: number;
  RECONCILIATION_ID?: number;
  INTERDATA_SALE_ID?: number;
  ACQ_PROVIDER?: string;
  ACQ_SALE_ID?: number;
  MATCH_RULE?: string;
  MATCH_META?: string;
  CREATED_AT?: string;
}

export interface ReconciliationDetails {
  reconciliation: ReconciliationRecord;
  match: ReconciliationMatch | null;
  interdataUi: {
    id: number | null;
    pdvNsu?: string | null;
    saleNo: string | null;
    datetime: string | null;
    grossAmount: number | null;
    netAmount: number | null;
    brand: string | null;
    status: string | null;
    paymentMethod?: string | null;
  } | null;
  acquirerUi: {
    provider: 'CIELO' | 'SIPAG' | 'SICREDI' | string;
    id: number | null;
    nsu: string | null;
    authCode: string | null;
    datetime: string | null;
    grossAmount: number | null;
    netAmount: number | null;
    brand: string | null;
    status: string | null;
    paymentMethod?: string | null;
    extra?: {
      saleCode?: string | null;
      tid?: string | null;
    };
  } | null;
  missing?: {
    match?: boolean;
    interdata?: boolean;
    acquirer?: boolean;
  };
  matchMeta?: Record<string, unknown> | string | null;
  interdataRaw?: Record<string, unknown> | null;
  acquirerRaw?: Record<string, unknown> | null;
}

export interface ReconciliationListResponse {
  ok: boolean;
  data: ReconciliationRecord[];
}

export interface AuditDuplicateFlags {
  erpDup: boolean;
  acqDup: boolean;
  reconDup: boolean;
  mismatch: boolean;
}

export interface AuditDuplicateKey {
  saleDate: string | null;
  methodGroup: string | null;
  grossAmount: number | null;
}

export interface AuditDuplicateRow {
  key: AuditDuplicateKey;
  erpCount: number;
  acqCount: number;
  reconCount: number;
  erpIds: number[];
  acqIds: number[];
  reconIds: number[];
  flags: AuditDuplicateFlags;
}

export interface AuditDuplicatesResponse {
  ok: boolean;
  data: AuditDuplicateRow[];
}
