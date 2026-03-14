import { CanonFlag, CanonicalTx } from '../canonical/types';
import { FlagConfig } from './config';

function absDiff(a: number, b: number) {
  return Math.abs(a - b);
}

function minutesBetween(aIso: string, bIso: string) {
  const a = new Date(aIso).getTime();
  const b = new Date(bIso).getTime();
  if (Number.isNaN(a) || Number.isNaN(b)) return Number.POSITIVE_INFINITY;
  return Math.abs(a - b) / 60000;
}

export function computePairFlags(
  erp: CanonicalTx | null,
  acq: CanonicalTx | null,
  cfg: FlagConfig,
): CanonFlag[] {
  const flags: CanonFlag[] = [];

  if (erp && !acq) return ['ERP_ONLY'];
  if (!erp && acq) return ['ACQ_ONLY'];
  if (!erp && !acq) return ['MISMATCH'];

  const isPix = erp!.methodGroup === 'PIX' || acq!.methodGroup === 'PIX';
  const timeTol = isPix ? cfg.pixTimeToleranceMinutes : cfg.timeToleranceMinutes;

  const moneyOk = absDiff(erp!.grossAmount, acq!.grossAmount) <= cfg.moneyToleranceAbs;
  const timeOk = minutesBetween(erp!.saleAt, acq!.saleAt) <= timeTol;

  if (moneyOk) flags.push('VALUE_TOLERANCE');
  if (timeOk) flags.push('TIME_TOLERANCE');

  if (erp!.methodGroup !== acq!.methodGroup) flags.push('METHOD_MISMATCH');
  if (erp!.brand && acq!.brand && erp!.brand !== acq!.brand) flags.push('BRAND_MISMATCH');

  const mismatch =
    !moneyOk || !timeOk || flags.includes('METHOD_MISMATCH') || flags.includes('BRAND_MISMATCH');
  flags.unshift(mismatch ? 'MISMATCH' : 'MATCH');

  return Array.from(new Set(flags));
}
