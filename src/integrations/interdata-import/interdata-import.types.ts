export type InterdataFileStatus = 'NEW' | 'PROCESSED' | 'ERROR' | 'SKIPPED_DUPLICATE';

export interface InterdataFileInfo {
  filename: string;
  size: number;
  mtimeMs: number;
}

export interface NormalizedSale {
  source: string;
  saleNo: string;
  saleDatetime: Date | null;
  authNsu?: string | null;
  cardBrandRaw?: string | null;
  paymentType: 'CARD' | 'PIX' | 'UNKNOWN';
  cardMode: 'CREDIT' | 'DEBIT' | 'UNKNOWN';
  installments: number | null;
  grossAmount: number | null;
  feesAmount: number | null;
  netAmount: number | null;
  statusRaw?: string | null;
  isCancelled: number;
  rowHash: string;
  invalidReason?: string;
}

export interface ParseResult {
  sales: NormalizedSale[];
  invalidSales: NormalizedSale[];
  meta: {
    totalRows: number;
    parsedRows: number;
    skippedRows: number;
    invalidRows: number;
    detectedColumns: Record<string, number | null>;
  };
}
