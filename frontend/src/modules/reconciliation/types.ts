export type StandardRow = {
  datetimeText: string;
  saleNoText: string;
  operatorText: string;
  operatorKey?: string;
  paymentText: string;
  brandText: string;
  nsuText: string;
  statusText: string;
  originText?: string;
  reasonText?: string;
  notesText?: string;
  totalText: string;
  feesText: string;
  netText: string;
  rowId?: number;
  raw?: any;
  provider?: string;
  matchLayer?: number | null;
  matchConfidence?: number | null;
  matchReason?: string | null;
};

export type StandardSource =
  | 'interdata'
  | 'cielo'
  | 'sipag'
  | 'reconciled'
  | 'acquirer'
  | string;
