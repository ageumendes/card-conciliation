export type EdiFileType = 'CIELO03' | 'CIELO04' | 'CIELO09' | 'CIELO15' | 'CIELO16' | 'UNKNOWN';

export type EdiFileStatus = 'NEW' | 'PROCESSED' | 'WARNING' | 'ERROR' | 'SKIPPED_DUPLICATE';

export interface EdiFileInfo {
  filename: string;
  size: number;
  mtimeMs: number;
  type?: EdiFileType;
}

export interface ParsedEdiFile {
  records: string[][];
  transactions?: EdiTransaction[];
  adjustments?: EdiAdjustment[];
  headerDate?: string | null;
  summary: {
    type: EdiFileType;
    lines: number;
    delimiter: string | null;
  };
}

export interface ParsedCielo04File {
  headerDate: string | null;
  transactions: EdiTransaction[];
  adjustmentBlocks: Cielo04AdjustmentBlock[];
}

export interface ParsedCielo16File {
  headerDate: string | null;
  headerEstablishment: string | null;
  transactions: EdiTransaction[];
  headerLine?: string | null;
  trailerLine?: string | null;
}

export interface EdiTransaction {
  saleDatetime?: string | null;
  establishmentNo: string | null;
  paymentMethod: string | null;
  brand: string | null;
  grossAmount: number | null;
  feeAmount: number | null;
  netAmount: number | null;
  status: string | null;
  entryType: string | null;
  reason: string | null;
  entryDate: Date | string | null;
  settlementDate: Date | string | null;
  authCode: string | null;
  nsuDoc: string | null;
  saleCode: string | null;
  tid: string | null;
  machineNumber: string | null;
  rowHash: string;
  pixId?: string | null;
  txId?: string | null;
  pixPaymentId?: string | null;
  eRecordType?: string | null;
  eSubmitEstablishment?: string | null;
  eLiquidationBrand?: string | null;
  eLiquidationType?: string | null;
  eInstallmentNo?: number | null;
  eInstallmentTotal?: number | null;
  eAuthCode?: string | null;
  eEntryType?: string | null;
  eChaveUr?: string | null;
  eNegotiationCode?: string | null;
  eAdjustmentCode?: string | null;
  ePaymentMethodCode?: string | null;
  eIndicatorPromo?: string | null;
  eIndicatorDcc?: string | null;
  eIndicatorMinCommission?: string | null;
  eIndicatorRaTc?: string | null;
  eIndicatorZeroFee?: string | null;
  eIndicatorRejected?: string | null;
  eIndicatorLateSale?: string | null;
  eCardBin?: string | null;
  eCardLast4?: string | null;
  eNsuDoc?: string | null;
  eInvoiceNo?: string | null;
  eTid?: string | null;
  eOrderReference?: string | null;
  eMdrRate?: number | null;
  eRaRate?: number | null;
  eSaleRate?: number | null;
  eTotalAmountSign?: string | null;
  eTotalAmount?: number | null;
  eGrossAmountSign?: string | null;
  eGrossAmount?: number | null;
  eNetAmountSign?: string | null;
  eNetAmount?: number | null;
  eCommissionSign?: string | null;
  eCommissionAmount?: number | null;
  eMinCommissionSign?: string | null;
  eMinCommissionAmount?: number | null;
  eEntrySign?: string | null;
  eEntryAmount?: number | null;
  eMdrFeeSign?: string | null;
  eMdrFeeAmount?: number | null;
  eFastReceiveSign?: string | null;
  eFastReceiveAmount?: number | null;
  eCashoutSign?: string | null;
  eCashoutAmount?: number | null;
  eShipmentFeeSign?: string | null;
  eShipmentFeeAmount?: number | null;
  ePendingSign?: string | null;
  ePendingAmount?: number | null;
  eDebtTotalSign?: string | null;
  eDebtTotalAmount?: number | null;
  eChargedSign?: string | null;
  eChargedAmount?: number | null;
  eAdminFeeSign?: string | null;
  eAdminFeeAmount?: number | null;
  ePromoSign?: string | null;
  ePromoAmount?: number | null;
  eDccSign?: string | null;
  eDccAmount?: number | null;
  eTimeHhmmss?: string | null;
  eCardGroup?: string | null;
  eReceiverDocument?: string | null;
  eAuthBrand?: string | null;
  eSaleUniqueCode?: string | null;
  eSaleOriginalCode?: string | null;
  eNegotiationEffectId?: string | null;
  eSalesChannel?: string | null;
  eTerminalLogicNo?: string | null;
  eOriginalEntryType?: string | null;
  eTransactionType?: string | null;
  eCieloUsage1?: string | null;
  ePricingModelCode?: string | null;
  eAuthDate?: string | null;
  eCaptureDate?: string | null;
  eEntryDate?: string | null;
  eOriginalEntryDate?: string | null;
  eBatchNumber?: string | null;
  eProcessedTransactionNo?: string | null;
  eRejectionReason?: string | null;
  eSettlementBlock?: string | null;
  eIndicatorCustomerInstallment?: string | null;
  eBank?: string | null;
  eBranch?: string | null;
  eAccount?: string | null;
  eAccountDigit?: string | null;
  eArn?: string | null;
  eIndicatorReceivableNegotiation?: string | null;
  eCaptureType?: string | null;
  eNegotiatorDocument?: string | null;
  eCieloUsage2?: string | null;
  eFileHeaderDate?: string | null;
  eRawLine?: string | null;
}

export interface EdiAdjustment {
  adjustmentDatetime: Date | null;
  establishmentNo: string | null;
  paymentMethod: string | null;
  brand: string | null;
  grossAmount: number | null;
  feeAmount: number | null;
  netAmount: number | null;
  entryType: string | null;
  reason: string | null;
  settlementDate: Date | string | null;
  authCode: string | null;
  nsuDoc: string | null;
  rowHash: string;
  fileName?: string | null;
  fileSeq?: number | null;
  fileDate?: string | null;
  lineNo?: number | null;
  ec?: string | null;
  adjTypeCode?: string | null;
  adjReasonCode?: string | null;
  adjGrossSign?: string | null;
  adjGross?: number | null;
  adjFeeSign?: string | null;
  adjFee?: number | null;
  adjNetSign?: string | null;
  adjNet?: number | null;
  referenceDate?: string | null;
  urKey?: string | null;
  nsuOriginal?: string | null;
  blockHash?: string | null;
  rawLine?: string | null;
}

export interface EdiAdjustmentItem {
  ajusteId?: number | null;
  saleId?: number | null;
  fileName?: string | null;
  lineNo?: number | null;
  ec?: string | null;
  nsuCielo?: string | null;
  authCode?: string | null;
  saleDate?: string | null;
  saleTime?: string | null;
  brandCode?: string | null;
  productCode?: string | null;
  cardBin?: string | null;
  cardLast4?: string | null;
  urKey?: string | null;
  gross?: number | null;
  net?: number | null;
  fee?: number | null;
  rawLine?: string | null;
  rowHash: string;
}

export interface Cielo04AdjustmentBlock {
  adjustment: EdiAdjustment;
  items: EdiAdjustmentItem[];
}
