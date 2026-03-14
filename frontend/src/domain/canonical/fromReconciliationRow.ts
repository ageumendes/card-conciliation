import { isoToDate, isoToTime, toIsoSafe } from './datetime';
import { normalizeMethodGroup } from './method';
import { CanonicalTx } from './types';

/**
 * Adapta uma row "reconciled" (T_RECONCILIATION) para 2 canônicos:
 * - ERP (campos principais)
 * - ACQ (campos ACQ_*)
 *
 * Isso permite computePairFlags(erp, acq) sem mexer no backend.
 */
export function canonPairFromReconciledRaw(raw: any): { erp: CanonicalTx; acq: CanonicalTx } {
  const erpIso = toIsoSafe(raw?.SALE_DATETIME ?? raw?.saleAt ?? raw?.saleDatetime);
  const acqRaw =
    raw?.ACQ_SALE_DATETIME ??
    raw?.acqSaleDatetime ??
    raw?.ACQ_DATETIME ??
    raw?.ACQ_CREATED_AT ??
    null;
  const acqIsoComputed = acqRaw ? toIsoSafe(acqRaw) : erpIso;
  const acqIso = acqIsoComputed.startsWith('1970-01-01') ? erpIso : acqIsoComputed;

  const erp: CanonicalTx = {
    id: `ERP:${raw?.INTERDATA_ID ?? raw?.INTERDATA_SALE_ID ?? raw?.ID ?? '0'}`,
    source: 'ERP',
    saleAt: erpIso,
    saleDate: isoToDate(erpIso),
    saleTime: isoToTime(erpIso),
    methodGroup: normalizeMethodGroup(
      raw?.CANON_METHOD_GROUP ?? raw?.METHOD_GROUP ?? raw?.PAYMENT_TYPE ?? raw?.PAYMENT_METHOD,
    ),
    method:
      String(raw?.CANON_METHOD ?? raw?.PAYMENT_METHOD ?? raw?.PAYMENT_TYPE ?? '').trim() ||
      undefined,
    brand: String(raw?.CANON_BRAND ?? raw?.BRAND ?? raw?.CARD_BRAND_RAW ?? '').trim() || undefined,
    grossAmount: Number(raw?.GROSS_AMOUNT ?? raw?.CANON_GROSS_AMOUNT ?? 0),
    netAmount: raw?.NET_AMOUNT != null ? Number(raw?.NET_AMOUNT) : undefined,
    nsu: String(raw?.CANON_NSU ?? raw?.NSU ?? raw?.AUTH_NSU ?? '').trim() || undefined,
    authCode: String(raw?.CANON_AUTH_CODE ?? raw?.AUTH_CODE ?? '').trim() || undefined,
    terminalNo:
      String(raw?.CANON_TERMINAL_NO ?? raw?.TERMINAL_NO ?? raw?.MACHINE_NUMBER ?? '').trim() ||
      undefined,
    status: String(raw?.STATUS ?? raw?.STATUS_RAW ?? '').trim() || undefined,
    rawRef: { table: 'T_RECONCILIATION(ERP)', originalId: raw?.ID },
    flags: [],
  };

  const acq: CanonicalTx = {
    id: `ACQ:${raw?.ACQUIRER_ID ?? raw?.ACQ_SALE_ID ?? raw?.ID ?? '0'}`,
    source: 'ACQ',
    saleAt: acqIso,
    saleDate: isoToDate(acqIso),
    saleTime: isoToTime(acqIso),
    methodGroup: normalizeMethodGroup(
      raw?.CANON_METHOD_GROUP ??
        raw?.ACQ_METHOD_GROUP ??
        raw?.ACQ_PAYMENT_METHOD ??
        raw?.PAYMENT_METHOD,
    ),
    method:
      String(raw?.ACQ_PAYMENT_METHOD ?? raw?.PAYMENT_METHOD ?? raw?.CANON_METHOD ?? '').trim() ||
      undefined,
    brand: String(raw?.ACQ_BRAND ?? raw?.BRAND ?? raw?.CANON_BRAND ?? '').trim() || undefined,
    grossAmount: Number(raw?.ACQ_GROSS_AMOUNT ?? raw?.GROSS_AMOUNT ?? raw?.CANON_GROSS_AMOUNT ?? 0),
    netAmount: raw?.ACQ_NET_AMOUNT != null ? Number(raw?.ACQ_NET_AMOUNT) : undefined,
    nsu: String(raw?.ACQ_NSU ?? raw?.NSU ?? raw?.CANON_NSU ?? '').trim() || undefined,
    authCode: String(raw?.ACQ_AUTH_CODE ?? raw?.AUTH_CODE ?? raw?.CANON_AUTH_CODE ?? '').trim() || undefined,
    terminalNo:
      String(raw?.ACQ_TERMINAL_NO ?? raw?.TERMINAL_NO ?? raw?.CANON_TERMINAL_NO ?? '').trim() ||
      undefined,
    status: String(raw?.ACQ_STATUS ?? raw?.STATUS ?? '').trim() || undefined,
    rawRef: { table: 'T_RECONCILIATION(ACQ)', originalId: raw?.ID },
    flags: [],
  };

  return { erp, acq };
}
