export type Source = 'ERP' | 'ACQ' | 'RECON';

export type MethodGroup = 'CARD' | 'PIX' | 'TEF' | 'CASH' | 'OTHER';

export type CanonFlag =
  | 'MATCH'
  | 'MISMATCH'
  | 'ERP_ONLY'
  | 'ACQ_ONLY'
  | 'RECON_ONLY'
  | 'VALUE_TOLERANCE'
  | 'TIME_TOLERANCE'
  | 'METHOD_MISMATCH'
  | 'BRAND_MISMATCH';

export interface CanonicalTx {
  id: string;
  source: Source;
  saleAt: string;
  saleDate: string;
  saleTime: string;
  methodGroup: MethodGroup;
  method?: string;
  brand?: string;
  grossAmount: number;
  netAmount?: number;
  nsu?: string;
  authCode?: string;
  terminalNo?: string;
  status?: string;
  rawRef?: { table?: string; originalId?: string | number };
  flags: CanonFlag[];
}
