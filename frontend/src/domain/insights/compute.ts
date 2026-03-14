import { CanonFlag, CanonicalTx, MethodGroup, Source } from '../canonical/types';

export interface Insights {
  totals: {
    count: number;
    bySource: Record<Source, number>;
    byMethodGroup: Record<MethodGroup, number>;
  };
  flags: {
    byFlag: Record<CanonFlag, number>;
    matchRate: number;
    mismatchRate: number;
  };
  brands: {
    byBrand: Record<string, number>;
  };
  timeline: {
    byDate: Record<string, number>;
  };
}

export function computeInsights(rows: CanonicalTx[]): Insights {
  const bySource: Record<Source, number> = { ERP: 0, ACQ: 0, RECON: 0 };
  const byMethodGroup: Record<MethodGroup, number> = {
    CARD: 0,
    PIX: 0,
    TEF: 0,
    CASH: 0,
    OTHER: 0,
  };
  const byFlag: Record<CanonFlag, number> = {
    MATCH: 0,
    MISMATCH: 0,
    ERP_ONLY: 0,
    ACQ_ONLY: 0,
    RECON_ONLY: 0,
    VALUE_TOLERANCE: 0,
    TIME_TOLERANCE: 0,
    METHOD_MISMATCH: 0,
    BRAND_MISMATCH: 0,
  };
  const byBrand: Record<string, number> = {};
  const byDate: Record<string, number> = {};

  let match = 0;
  let mismatch = 0;

  for (const r of rows) {
    bySource[r.source] = (bySource[r.source] ?? 0) + 1;
    byMethodGroup[r.methodGroup] = (byMethodGroup[r.methodGroup] ?? 0) + 1;

    if (r.brand) byBrand[r.brand] = (byBrand[r.brand] ?? 0) + 1;
    if (r.saleDate) byDate[r.saleDate] = (byDate[r.saleDate] ?? 0) + 1;

    for (const f of r.flags ?? []) {
      byFlag[f] = (byFlag[f] ?? 0) + 1;
    }

    if (r.flags?.includes('MATCH')) match++;
    if (r.flags?.includes('MISMATCH')) mismatch++;
  }

  const count = rows.length || 1;
  return {
    totals: { count: rows.length, bySource, byMethodGroup },
    flags: {
      byFlag,
      matchRate: match / count,
      mismatchRate: mismatch / count,
    },
    brands: { byBrand },
    timeline: { byDate },
  };
}
