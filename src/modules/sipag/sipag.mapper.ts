import { NormalizedTransaction, SipagTransactionRaw } from './sipag.types';

const toNumber = (value?: number | string): number | undefined => {
  if (value === undefined || value === null || value === '') {
    return undefined;
  }
  if (typeof value === 'number' && !Number.isNaN(value)) {
    return Number(value.toFixed(2));
  }
  const raw = String(value).trim();
  if (!raw) {
    return undefined;
  }
  const normalized = raw
    .replace(/\s/g, '')
    .replace('R$', '')
    .replace(/\./g, '')
    .replace(',', '.');
  const parsed = Number(normalized);
  return Number.isNaN(parsed) ? undefined : Number(parsed.toFixed(2));
};

export const mapSipagToNormalized = (
  raw: SipagTransactionRaw,
  refDate: string,
): NormalizedTransaction => {
  const gross = toNumber(raw.grossAmount ?? (raw as any).gross_amount);
  const net = toNumber(raw.netAmount ?? (raw as any).net_amount);
  const fee = toNumber(raw.feeAmount ?? (raw as any).fee_amount);

  return {
    refDate,
    extId: raw.extId ?? raw.id ?? ((raw as any).ext_id as string | undefined),
    nsu: raw.nsu ?? ((raw as any).nsu_host as string | undefined),
    authCode: raw.authCode ?? ((raw as any).auth_code as string | undefined),
    tid: raw.tid ?? ((raw as any).terminal_id as string | undefined),
    grossAmount: gross,
    netAmount: net,
    feeAmount: fee ?? (gross !== undefined && net !== undefined ? gross - net : undefined),
    brand: raw.brand ?? ((raw as any).card_brand as string | undefined),
    installments: toNumber(raw.installments ?? (raw as any).installments_count),
    statusAcq: raw.statusAcq ?? raw.status ?? ((raw as any).status_acq as string | undefined),
    capturedAt: raw.capturedAt ?? ((raw as any).captured_at as string | undefined),
    settlementDate: raw.settlementDate ?? ((raw as any).settlement_date as string | undefined),
  };
};
