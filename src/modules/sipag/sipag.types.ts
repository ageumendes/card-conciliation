export interface SipagTransactionRaw {
  id?: string;
  extId?: string;
  nsu?: string;
  authCode?: string;
  tid?: string;
  grossAmount?: number | string;
  netAmount?: number | string;
  feeAmount?: number | string;
  brand?: string;
  installments?: number | string;
  status?: string;
  statusAcq?: string;
  capturedAt?: string;
  settlementDate?: string;
  [key: string]: unknown;
}

export interface SipagExtractResponse {
  data?: SipagTransactionRaw[];
  transactions?: SipagTransactionRaw[];
  page?: number;
  totalPages?: number;
  nextPage?: number;
  [key: string]: unknown;
}

export interface NormalizedTransaction {
  refDate: string;
  extId?: string;
  nsu?: string;
  authCode?: string;
  tid?: string;
  grossAmount?: number;
  netAmount?: number;
  feeAmount?: number;
  brand?: string;
  installments?: number;
  statusAcq?: string;
  capturedAt?: string;
  settlementDate?: string;
}
