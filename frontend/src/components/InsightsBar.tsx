import { useEffect, useMemo, useState } from 'react';
import { CanonicalTx } from '../domain/canonical/types';
import { computeInsights } from '../domain/insights/compute';
import { Card } from './common/Card';
import { BrandLogo } from './BrandLogo';
import { AcquirerLogo } from './AcquirerLogo';
import { formatMoneyBRL } from '../modules/reconciliation/normalizeRow';

function pct(v: number) {
  if (!Number.isFinite(v)) return '0%';
  return `${(v * 100).toFixed(2)}%`;
}

function topEntries(obj: Record<string, number>, n: number) {
  return Object.entries(obj)
    .sort((a, b) => (b[1] ?? 0) - (a[1] ?? 0))
    .slice(0, n);
}

export function InsightsBar({
  rows,
  financialRows = [],
  comparisonRows = [],
  showBrandAnalysisCard = true,
  showComparisonCard = true,
  showTopBrandsCard = true,
  brandAnalysisVertical = false,
  fillHeight = false,
}: {
  rows: CanonicalTx[];
  financialRows?: Array<Record<string, unknown>>;
  comparisonRows?: Array<Record<string, unknown>>;
  showBrandAnalysisCard?: boolean;
  showComparisonCard?: boolean;
  showTopBrandsCard?: boolean;
  brandAnalysisVertical?: boolean;
  fillHeight?: boolean;
}) {
  const insights = useMemo(() => computeInsights(rows), [rows]);
  const headerAcquirers = useMemo(() => {
    const knownOrder = ['CIELO', 'SIPAG', 'SICREDI'];
    const unique = Array.from(
      new Set(
        financialRows
          .map((row) => String(row.ACQUIRER ?? row.ACQ_PROVIDER ?? '').trim().toUpperCase())
          .filter(Boolean),
      ),
    );
    return unique.sort((a, b) => {
      const ia = knownOrder.indexOf(a);
      const ib = knownOrder.indexOf(b);
      if (ia !== -1 || ib !== -1) {
        return (ia === -1 ? 999 : ia) - (ib === -1 ? 999 : ib);
      }
      return a.localeCompare(b, 'pt-BR');
    });
  }, [financialRows]);

  const brandFinance = useMemo(() => {
    const map = new Map<
      string,
      { brand: string; gross: number; net: number; taxSum: number; taxCount: number; count: number }
    >();
    for (const row of financialRows) {
      const brand = String(
        row.ACQ_CANON_BRAND_RESOLVED ?? row.ACQ_BRAND_RESOLVED ?? row.CANON_BRAND ?? row.BRAND ?? '',
      )
        .trim()
        .toUpperCase();
      if (!brand) {
        continue;
      }
      const grossRaw = row.ACQ_GROSS_AMOUNT ?? row.GROSS_AMOUNT;
      const netRaw = row.ACQ_NET_AMOUNT ?? row.NET_AMOUNT;
      const feeRaw = row.ACQ_FEE_AMOUNT;
      const percRaw = row.ACQ_PERC_TAXA;
      const gross = typeof grossRaw === 'number' ? grossRaw : Number(grossRaw ?? NaN);
      const net = typeof netRaw === 'number' ? netRaw : Number(netRaw ?? NaN);
      if (!Number.isFinite(gross) || gross <= 0) {
        continue;
      }
      let taxPerc =
        typeof percRaw === 'number'
          ? percRaw
          : Number(percRaw ?? NaN);
      if (!Number.isFinite(taxPerc)) {
        const fee = typeof feeRaw === 'number' ? feeRaw : Number(feeRaw ?? NaN);
        if (Number.isFinite(fee)) {
          taxPerc = (fee / gross) * 100;
        } else if (Number.isFinite(net)) {
          taxPerc = ((gross - net) / gross) * 100;
        }
      }
      const current = map.get(brand) ?? {
        brand,
        gross: 0,
        net: 0,
        taxSum: 0,
        taxCount: 0,
        count: 0,
      };
      current.gross += gross;
      if (Number.isFinite(net)) {
        current.net += net;
      } else {
        current.net += gross;
      }
      if (Number.isFinite(taxPerc)) {
        current.taxSum += Math.abs(taxPerc);
        current.taxCount += 1;
      }
      current.count += 1;
      map.set(brand, current);
    }
    return Array.from(map.values())
      .map((item) => ({
        ...item,
        avgTaxPerc: item.taxCount ? item.taxSum / item.taxCount : null,
      }))
      .sort((a, b) => b.gross - a.gross);
  }, [financialRows]);
  const brandAcquirerTaxComparison = useMemo(() => {
    type Acc = { sum: number; count: number };
    const acc = new Map<string, Map<string, Acc>>();

    for (const row of comparisonRows) {
      const acquirer = String(row.ACQUIRER ?? row.ACQ_PROVIDER ?? '')
        .trim()
        .toUpperCase();
      const brand = String(
        row.ACQ_CANON_BRAND_RESOLVED ?? row.ACQ_BRAND_RESOLVED ?? row.CANON_BRAND ?? row.BRAND ?? '',
      )
        .trim()
        .toUpperCase();
      if (!acquirer || !brand) {
        continue;
      }

      const grossRaw = row.ACQ_GROSS_AMOUNT ?? row.GROSS_AMOUNT;
      const feeRaw = row.ACQ_FEE_AMOUNT;
      const netRaw = row.ACQ_NET_AMOUNT ?? row.NET_AMOUNT;
      const percRaw = row.ACQ_PERC_TAXA;
      const gross = typeof grossRaw === 'number' ? grossRaw : Number(grossRaw ?? NaN);
      let perc = typeof percRaw === 'number' ? percRaw : Number(percRaw ?? NaN);
      if (!Number.isFinite(perc) && Number.isFinite(gross) && gross !== 0) {
        const fee = typeof feeRaw === 'number' ? feeRaw : Number(feeRaw ?? NaN);
        const net = typeof netRaw === 'number' ? netRaw : Number(netRaw ?? NaN);
        if (Number.isFinite(fee)) {
          perc = (fee / gross) * 100;
        } else if (Number.isFinite(net)) {
          perc = ((gross - net) / gross) * 100;
        }
      }
      if (!Number.isFinite(perc)) {
        continue;
      }

      const byAcquirer = acc.get(brand) ?? new Map<string, Acc>();
      const slot = byAcquirer.get(acquirer) ?? { sum: 0, count: 0 };
      slot.sum += Math.abs(perc);
      slot.count += 1;
      byAcquirer.set(acquirer, slot);
      acc.set(brand, byAcquirer);
    }

    const list = Array.from(acc.entries()).map(([brand, byAcquirer]) => {
      const averages = Array.from(byAcquirer.entries()).map(([acquirer, value]) => ({
        acquirer,
        avg: value.count ? value.sum / value.count : 0,
        count: value.count,
      }));
      const values = averages.map((item) => item.avg);
      const spread = values.length > 1 ? Math.max(...values) - Math.min(...values) : values[0] ?? 0;
      return { brand, averages, spread };
    });

    return list
      .sort((a, b) => b.spread - a.spread || b.averages.length - a.averages.length)
      .slice(0, 6);
  }, [comparisonRows]);
  const [brandIndex, setBrandIndex] = useState(0);
  const [comparisonIndex, setComparisonIndex] = useState(0);
  const [pauseBrandCarousel, setPauseBrandCarousel] = useState(false);
  const [pauseComparisonCarousel, setPauseComparisonCarousel] = useState(false);

  useEffect(() => {
    if (!brandFinance.length) {
      setBrandIndex(0);
      return;
    }
    setBrandIndex((current) => Math.min(current, brandFinance.length - 1));
  }, [brandFinance.length]);

  useEffect(() => {
    if (brandFinance.length <= 1) {
      return;
    }
    const timer = window.setInterval(() => {
      if (pauseBrandCarousel) {
        return;
      }
      setBrandIndex((current) => (current + 1) % brandFinance.length);
    }, 3500);
    return () => window.clearInterval(timer);
  }, [brandFinance.length, pauseBrandCarousel]);

  useEffect(() => {
    if (!brandAcquirerTaxComparison.length) {
      setComparisonIndex(0);
      return;
    }
    setComparisonIndex((current) => Math.min(current, brandAcquirerTaxComparison.length - 1));
  }, [brandAcquirerTaxComparison.length]);

  useEffect(() => {
    if (brandAcquirerTaxComparison.length <= 1) {
      return;
    }
    const timer = window.setInterval(() => {
      if (pauseComparisonCarousel) {
        return;
      }
      setComparisonIndex((current) => (current + 1) % brandAcquirerTaxComparison.length);
    }, 3500);
    return () => window.clearInterval(timer);
  }, [brandAcquirerTaxComparison.length, pauseComparisonCarousel]);

  const activeBrand = brandFinance[brandIndex] ?? null;

  const topBrands = topEntries(insights.brands.byBrand, 5);
  const visibleCardsCount =
    (showBrandAnalysisCard ? 1 : 0) +
    (showComparisonCard ? 1 : 0) +
    (showTopBrandsCard ? 1 : 0);
  const gridClassName =
    visibleCardsCount === 3
      ? 'grid grid-cols-1 gap-3 md:[grid-template-columns:2.1fr_1fr_0.9fr]'
      : visibleCardsCount === 2
        ? 'grid grid-cols-1 gap-3 md:grid-cols-2'
        : 'grid grid-cols-1 gap-3';

  return (
    <div className={`${gridClassName} ${fillHeight ? 'h-full' : ''}`}>
      {showBrandAnalysisCard && (
      <Card className={fillHeight ? 'h-full' : undefined}>
        <div
          className="h-full p-4"
          onMouseEnter={() => setPauseBrandCarousel(true)}
          onMouseLeave={() => setPauseBrandCarousel(false)}
        >
          <div className="flex min-h-5 items-center gap-2">
            {headerAcquirers.length ? (
              headerAcquirers.map((acquirer) => (
                <AcquirerLogo key={acquirer} acquirer={acquirer} className="h-5" />
              ))
            ) : (
              <span className="text-xs uppercase text-slate-500">Sem adquirente</span>
            )}
          </div>
          <div className="mt-2 text-[11px] text-slate-700">
            {activeBrand ? (
              <div className="rounded border border-slate-200 bg-slate-50 px-2 py-2">
                <div className="mb-2 flex items-center justify-between gap-2">
                  <BrandLogo brand={activeBrand.brand} className="h-4" />
                  <span className="text-[10px] text-slate-500">
                    itens: {activeBrand.count}
                  </span>
                </div>
                <div className={`grid gap-2 ${brandAnalysisVertical ? 'grid-cols-1' : 'grid-cols-3'}`}>
                  <div className={brandAnalysisVertical ? 'border-b border-slate-200 pb-1' : ''}>
                    <div className="text-[10px] uppercase text-slate-500">Bruto</div>
                    <div className="font-semibold">{formatMoneyBRL(activeBrand.gross)}</div>
                  </div>
                  <div className={brandAnalysisVertical ? 'border-b border-slate-200 pb-1' : ''}>
                    <div className="text-[10px] uppercase text-slate-500">%Taxa</div>
                    <div className="font-semibold text-red-600">
                      {activeBrand.avgTaxPerc === null
                        ? '--'
                        : `${activeBrand.avgTaxPerc.toFixed(2)}%`}
                    </div>
                  </div>
                  <div>
                    <div className="text-[10px] uppercase text-slate-500">Liquido</div>
                    <div className="font-semibold">{formatMoneyBRL(activeBrand.net)}</div>
                  </div>
                </div>

                <div className="mt-3 flex items-center justify-between">
                  <button
                    type="button"
                    className="rounded border px-2 py-0.5 text-[10px] hover:bg-slate-100"
                    onClick={() =>
                      setBrandIndex((current) =>
                        (current - 1 + brandFinance.length) % brandFinance.length,
                      )
                    }
                  >
                    Anterior
                  </button>
                  <span className="text-[10px] text-slate-500">
                    {brandIndex + 1} / {brandFinance.length}
                  </span>
                  <div className="flex items-center gap-1">
                    <button
                      type="button"
                      className="rounded border px-2 py-0.5 text-[10px] hover:bg-slate-100"
                      onClick={() =>
                        setBrandIndex((current) => (current + 1) % brandFinance.length)
                      }
                    >
                      Proximo
                    </button>
                    <button
                      type="button"
                      className="rounded border px-2 py-0.5 text-[10px] hover:bg-slate-100 md:hidden"
                      onClick={() => setPauseBrandCarousel((current) => !current)}
                    >
                      {pauseBrandCarousel ? 'Continuar' : 'Pausar'}
                    </button>
                  </div>
                </div>
              </div>
            ) : (
              <div className="text-slate-500">Sem dados</div>
            )}
          </div>
        </div>
      </Card>
      )}

      {showComparisonCard && (
      <Card className={fillHeight ? 'h-full' : undefined}>
        <div
          className={`p-4 ${fillHeight ? 'h-full' : ''}`}
          onMouseEnter={() => setPauseComparisonCarousel(true)}
          onMouseLeave={() => setPauseComparisonCarousel(false)}
        >
          <div className="flex items-center justify-between">
            <div className="text-xs uppercase text-slate-500">Comparativo de Taxas</div>
            <button
              type="button"
              className="rounded border px-2 py-0.5 text-[10px] hover:bg-slate-100 md:hidden"
              onClick={() => setPauseComparisonCarousel((current) => !current)}
            >
              {pauseComparisonCarousel ? 'Continuar' : 'Pausar'}
            </button>
          </div>
          <div className="mt-2 h-28 overflow-hidden text-xs text-slate-700">
            {brandAcquirerTaxComparison.length ? (
              <div
                className="transition-transform duration-500 ease-in-out"
                style={{ transform: `translateY(-${comparisonIndex * 7}rem)` }}
              >
                {brandAcquirerTaxComparison.map((entry) => (
                  <div
                    key={entry.brand}
                    className="h-28 rounded border border-slate-200 bg-slate-50 px-2 py-1"
                  >
                    <div className="mb-1 flex items-center justify-between">
                      <BrandLogo brand={entry.brand} className="h-3" />
                      <span className="font-semibold text-red-600">{entry.spread.toFixed(2)}%</span>
                    </div>
                    <div className="space-y-0.5">
                      {entry.averages
                        .sort((a, b) => b.avg - a.avg)
                        .map((item, index, list) => (
                          <div
                            key={`${entry.brand}-${item.acquirer}`}
                            className={`flex items-center justify-between py-0.5 ${
                              index < list.length - 1 ? 'border-b border-slate-200' : ''
                            }`}
                          >
                            <AcquirerLogo acquirer={item.acquirer} className="h-3" />
                            <span>{item.avg.toFixed(2)}%</span>
                          </div>
                        ))}
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-slate-500">Sem dados</div>
            )}
          </div>
          {!!brandAcquirerTaxComparison.length && (
            <div className="mt-2 text-[10px] text-slate-500">
              {comparisonIndex + 1} / {brandAcquirerTaxComparison.length}
            </div>
          )}
        </div>
      </Card>
      )}

      {showTopBrandsCard && (
      <Card className={fillHeight ? 'h-full' : undefined}>
        <div className={`p-4 ${fillHeight ? 'h-full' : ''}`}>
          <div className="text-xs uppercase text-slate-500">Top Bandeiras</div>
          <div className="mt-2 space-y-1 text-xs text-slate-700">
            {topBrands.map(([k, v], index) => (
              <div
                key={k}
                className={`flex items-center justify-between py-1 ${
                  index < topBrands.length - 1 ? 'border-b border-slate-200' : ''
                }`}
              >
                <span className="font-semibold" title={k}>
                  <BrandLogo brand={k} className="h-4" />
                </span>
                <span className="text-slate-500">{v} vendas</span>
              </div>
            ))}
            {!topBrands.length && <div className="text-slate-500">Sem dados</div>}
          </div>
        </div>
      </Card>
      )}
    </div>
  );
}
