import { IsEnum, IsInt, IsNotEmpty, IsOptional, IsString, Min } from 'class-validator';

export enum ManualReconciliationProvider {
  CIELO = 'CIELO',
  SIPAG = 'SIPAG',
  CIELO_EDI = 'CIELO_EDI',
}

export enum ManualReconciliationReason {
  TIME_DIFF = 'TIME_DIFF',
  NSU_DIFF = 'NSU_DIFF',
  BATCH = 'BATCH',
  OTHER = 'OTHER',
}

export class ManualReconciliationDto {
  @IsInt()
  @Min(1)
  interdataId!: number;

  @IsEnum(ManualReconciliationProvider)
  acquirerProvider!: ManualReconciliationProvider;

  @IsInt()
  @Min(1)
  acquirerSaleId!: number;

  @IsEnum(ManualReconciliationReason)
  reason!: ManualReconciliationReason;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  notes?: string;
}

export type DuplicateKey = {
  canonSaleDate?: string | null;
  canonMethodGroup?: string | null;
  canonGrossAmount?: number | null;
  canonTerminalNo?: string | null;
  canonNsu?: string | null;
  canonAuthCode?: string | null;
};

export type DuplicateGroup = {
  key: DuplicateKey;
  count: number;
  ids: number[];
  sampleSaleDatetimeMin?: string | Date | null;
  sampleSaleDatetimeMax?: string | Date | null;
};

export type AuditDuplicateKey = {
  saleDate: string | null;
  methodGroup: string | null;
  grossAmount: number | null;
};

export type AuditDuplicateRow = {
  key: AuditDuplicateKey;
  erpCount: number;
  acqCount: number;
  reconCount: number;
  erpIds: number[];
  acqIds: number[];
  reconIds: number[];
  flags: {
    erpDup: boolean;
    acqDup: boolean;
    reconDup: boolean;
    mismatch: boolean;
  };
};

export type AuditDuplicateRowRaw = {
  SALE_DATE?: string | null;
  METHOD_GROUP?: string | null;
  GROSS_AMOUNT?: number | string | null;
  ERP_COUNT?: number | null;
  ACQ_COUNT?: number | null;
  RECON_COUNT?: number | null;
  ERP_IDS?: string | null;
  ACQ_IDS?: string | null;
  RECON_IDS?: string | null;
};
