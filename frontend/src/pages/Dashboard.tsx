import { useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { InsightsBar } from '../components/InsightsBar';
import { fetchAcquirerFinance } from '../api/acquirerImport';
import { CanonicalTx } from '../domain/canonical/types';
import { Card } from '../components/common/Card';
import { AcquirerLogo } from '../components/AcquirerLogo';
import { formatDatetimeCompact, formatMoneyBRL } from '../modules/reconciliation/normalizeRow';

function ymdLocal(d: Date) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

function daysAgo(base: Date, n: number) {
  const d = new Date(base.getFullYear(), base.getMonth(), base.getDate());
  d.setDate(d.getDate() - n);
  return d;
}

type RangePreset = 'today' | 'yesterday' | '7d' | '30d' | 'custom';
type AcquirerFilter = 'ALL' | 'CIELO' | 'SIPAG' | 'SICREDI';

export const Dashboard = () => {
  const [rangePreset, setRangePreset] = useState<RangePreset>('7d');
  const [acquirerFilter, setAcquirerFilter] = useState<AcquirerFilter>('ALL');
  const [dateFrom, setDateFrom] = useState(() => ymdLocal(daysAgo(new Date(), 7)));
  const [dateTo, setDateTo] = useState(() => ymdLocal(new Date()));
  const [selectedTaxCard, setSelectedTaxCard] = useState<{
    acquirer: string;
    method: string;
  } | null>(null);
  const [tablePage, setTablePage] = useState(1);
  const [tablePageSize, setTablePageSize] = useState(50);

  useEffect(() => {
    const now = new Date();
    if (rangePreset === 'today') {
      const day = ymdLocal(now);
      setDateFrom(day);
      setDateTo(day);
      return;
    }
    if (rangePreset === 'yesterday') {
      const day = ymdLocal(daysAgo(now, 1));
      setDateFrom(day);
      setDateTo(day);
      return;
    }
    if (rangePreset === '30d') {
      setDateFrom(ymdLocal(daysAgo(now, 30)));
      setDateTo(ymdLocal(now));
      return;
    }
    if (rangePreset === '7d') {
      setDateFrom(ymdLocal(daysAgo(now, 7)));
      setDateTo(ymdLocal(now));
    }
  }, [rangePreset]);

  const {
    data: financialSalesResp,
    isLoading,
    isError,
    refetch,
    isFetching,
  } = useQuery({
    queryKey: ['dashboard-finance', dateFrom, dateTo, acquirerFilter],
    queryFn: () =>
      fetchAcquirerFinance({
        acquirers: acquirerFilter === 'ALL' ? 'cielo,sipag,sicredi' : acquirerFilter.toLowerCase(),
        dateFrom,
        dateTo,
        sortBy: 'datetime',
        sortDir: 'desc',
        includeReconciled: true,
      }),
    enabled: Boolean(dateFrom && dateTo),
  });

  const financialRows = useMemo(
    () => {
      const source = (((financialSalesResp as any)?.data ?? []) as Array<Record<string, unknown>>);
      const toNumberOrNull = (value: unknown): number | null => {
        if (typeof value === 'number') {
          return Number.isFinite(value) ? value : null;
        }
        if (value === null || value === undefined || value === '') {
          return null;
        }
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : null;
      };
      return source.map((item) => {
        const raw = (item.raw ?? {}) as Record<string, unknown>;
        const provider = String(item.acquirer ?? raw.ACQUIRER ?? raw.ACQ_PROVIDER ?? 'N/A').toUpperCase();
        const gross =
          toNumberOrNull(raw.CANON_GROSS_AMOUNT) ??
          toNumberOrNull(raw.GROSS_AMOUNT) ??
          toNumberOrNull(item.grossAmount);
        const net =
          toNumberOrNull(raw.CANON_NET_AMOUNT) ??
          toNumberOrNull(raw.NET_AMOUNT) ??
          toNumberOrNull(item.netAmount);
        const fee =
          toNumberOrNull(raw.CANON_FEE_AMOUNT) ??
          toNumberOrNull(raw.FEE_AMOUNT) ??
          toNumberOrNull(item.mdrAmount) ??
          (gross !== null && net !== null ? gross - net : null);
        const perc =
          toNumberOrNull(raw.CANON_PERC_TAXA) ??
          (gross !== null && gross !== 0 && fee !== null ? (fee / gross) * 100 : null);
        const canonMethodRaw = String(raw.CANON_METHOD ?? '').trim().toUpperCase();
        const canonMethodGroupRaw = String(raw.CANON_METHOD_GROUP ?? '').trim().toUpperCase();
        const method =
          String(
            raw.CANON_METHOD ??
              raw.PAYMENT_METHOD ??
              raw.CREDIT_DEBIT_IND ??
              raw.PRODUCT ??
              raw.CARD_TYPE ??
              '',
          ).trim() || null;
        const methodUpper = String(method ?? canonMethodRaw).toUpperCase();
        const methodGroup =
          canonMethodGroupRaw ||
          (methodUpper.includes('PIX') ? 'PIX' : methodUpper ? 'CARD' : '');
        const brand =
          String(raw.CANON_BRAND ?? raw.BRAND ?? item.brand ?? '').trim() || null;
        return {
          ...raw,
          ID: raw.ID ?? item.id,
          ACQUIRER: provider,
          ACQ_PROVIDER: provider,
          SALE_DATETIME: raw.SALE_DATETIME ?? item.saleDatetime ?? null,
          STATUS: raw.STATUS ?? item.status ?? null,
          BRAND: raw.BRAND ?? item.brand ?? null,
          ACQ_GROSS_AMOUNT: gross,
          ACQ_NET_AMOUNT: net,
          ACQ_FEE_AMOUNT: fee,
          ACQ_PERC_TAXA: perc,
          ACQ_CANON_METHOD_GROUP_RESOLVED: methodGroup || null,
          ACQ_CANON_METHOD_RESOLVED: method,
          ACQ_CANON_BRAND_RESOLVED: brand,
        } as Record<string, unknown>;
      });
    },
    [financialSalesResp],
  );

  const financialCanonRows: CanonicalTx[] = useMemo(() => {
    const toDateParts = (value: unknown) => {
      if (!value) {
        return { saleAt: '', saleDate: '', saleTime: '' };
      }
      const date = new Date(String(value));
      if (Number.isNaN(date.getTime())) {
        return { saleAt: String(value), saleDate: '', saleTime: '' };
      }
      const pad = (n: number) => String(n).padStart(2, '0');
      const yyyy = date.getFullYear();
      const mm = pad(date.getMonth() + 1);
      const dd = pad(date.getDate());
      const hh = pad(date.getHours());
      const mi = pad(date.getMinutes());
      const ss = pad(date.getSeconds());
      return {
        saleAt: `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`,
        saleDate: `${yyyy}-${mm}-${dd}`,
        saleTime: `${hh}:${mi}:${ss}`,
      };
    };
    return financialRows.map((row, index) => {
      const methodGroupRaw = String(
        row.ACQ_CANON_METHOD_GROUP_RESOLVED ?? row.CANON_METHOD_GROUP ?? '',
      ).toUpperCase();
      const methodRaw = String(
        row.ACQ_CANON_METHOD_RESOLVED ?? row.CANON_METHOD ?? '',
      ).toUpperCase();
      const methodGroup = methodGroupRaw.includes('PIX') || methodRaw.includes('PIX') ? 'PIX' : 'CARD';
      const grossRaw = row.ACQ_GROSS_AMOUNT ?? row.GROSS_AMOUNT;
      const netRaw = row.ACQ_NET_AMOUNT ?? row.NET_AMOUNT;
      const gross = typeof grossRaw === 'number' ? grossRaw : Number(grossRaw ?? 0);
      const net = typeof netRaw === 'number' ? netRaw : Number(netRaw ?? NaN);
      const { saleAt, saleDate, saleTime } = toDateParts(row.SALE_DATETIME ?? row.CREATED_AT);
      return {
        id: String(row.ID ?? index),
        source: 'ACQ',
        saleAt,
        saleDate,
        saleTime,
        methodGroup,
        method: methodRaw || undefined,
        brand: String(row.ACQ_CANON_BRAND_RESOLVED ?? row.BRAND ?? '').toUpperCase() || undefined,
        grossAmount: Number.isFinite(gross) ? gross : 0,
        netAmount: Number.isFinite(net) ? net : undefined,
        status: String(row.STATUS ?? '') || undefined,
        terminalNo: String(row.CANON_TERMINAL_NO ?? row.TERMINAL_NO ?? '') || undefined,
        authCode: String(row.CANON_AUTH_CODE ?? row.AUTH_CODE ?? row.AUTH_NO ?? '') || undefined,
        nsu: String(row.CANON_NSU ?? row.NSU_DOC ?? row.TRANSACTION_NO ?? '') || undefined,
        flags: [],
      } as CanonicalTx;
    });
  }, [financialRows]);

  const finance = useMemo(() => {
    let gross = 0;
    let net = 0;
    let fee = 0;
    let feeCount = 0;
    let withoutFeeCount = 0;
    for (const row of financialRows) {
      const grossRaw = row.ACQ_GROSS_AMOUNT ?? row.GROSS_AMOUNT;
      const netRaw = row.ACQ_NET_AMOUNT ?? row.NET_AMOUNT;
      const feeRaw = row.ACQ_FEE_AMOUNT;
      const grossNum = typeof grossRaw === 'number' ? grossRaw : Number(grossRaw ?? 0);
      const netNum = typeof netRaw === 'number' ? netRaw : Number(netRaw ?? 0);
      const feeNum =
        typeof feeRaw === 'number'
          ? feeRaw
          : Number.isFinite(grossNum) && Number.isFinite(netNum)
            ? Number((grossNum - netNum).toFixed(2))
            : NaN;
      if (Number.isFinite(grossNum)) {
        gross += grossNum;
      }
      if (Number.isFinite(netNum)) {
        net += netNum;
      }
      if (Number.isFinite(feeNum)) {
        fee += Math.abs(feeNum);
        feeCount += 1;
      } else {
        withoutFeeCount += 1;
      }
    }
    const ticketMedio = financialRows.length ? gross / financialRows.length : 0;
    const feePerc = gross ? (fee / gross) * 100 : 0;
    const feeCoverage = financialRows.length ? (feeCount / financialRows.length) * 100 : 0;
    return { gross, net, fee, feePerc, ticketMedio, feeCount, withoutFeeCount, feeCoverage };
  }, [financialRows]);

  const byAcquirer = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const row of financialRows) {
      const provider = String(row.ACQUIRER ?? row.ACQ_PROVIDER ?? 'N/A').toUpperCase();
      counts[provider] = (counts[provider] ?? 0) + 1;
    }
    const total = Object.values(counts).reduce((sum, value) => sum + value, 0);
    return Object.entries(counts)
      .sort((a, b) => b[1] - a[1])
      .map(([name, value]) => ({
        name,
        value,
        pct: total ? (value / total) * 100 : 0,
      }));
  }, [financialRows]);

  const avgFeesByAcquirerMethod = useMemo(() => {
    const allAcquirers = ['CIELO', 'SIPAG', 'SICREDI'] as const;
    const methods = ['CREDITO', 'DEBITO', 'VOUCHER', 'PIX'] as const;
    const buckets = new Map<string, { acquirer: string; method: string; sum: number; count: number }>();
    const normalizeMethod = (value: unknown) => {
      const raw = String(value ?? '').trim().toUpperCase();
      if (!raw) {
        return '';
      }
      if (raw.includes('DEBIT')) {
        return 'DEBITO';
      }
      if (raw.includes('CRED')) {
        return 'CREDITO';
      }
      if (raw.includes('VOUCHER')) {
        return 'VOUCHER';
      }
      if (raw.includes('PIX')) {
        return 'PIX';
      }
      return raw;
    };

    for (const row of financialRows) {
      const acquirer = String(row.ACQUIRER ?? row.ACQ_PROVIDER ?? 'N/A').toUpperCase();
      const method = normalizeMethod(row.ACQ_CANON_METHOD_RESOLVED ?? row.CANON_METHOD);
      if (!acquirer || !method) {
        continue;
      }
      const percRaw = row.ACQ_PERC_TAXA;
      const grossRaw = row.ACQ_GROSS_AMOUNT ?? row.GROSS_AMOUNT;
      const feeRaw = row.ACQ_FEE_AMOUNT;
      const netRaw = row.ACQ_NET_AMOUNT ?? row.NET_AMOUNT;

      const perc =
        typeof percRaw === 'number'
          ? percRaw
          : Number(percRaw ?? NaN);
      let value = Number.isFinite(perc) ? Math.abs(perc) : NaN;

      if (!Number.isFinite(value)) {
        const gross = typeof grossRaw === 'number' ? grossRaw : Number(grossRaw ?? NaN);
        const feeDirect = typeof feeRaw === 'number' ? feeRaw : Number(feeRaw ?? NaN);
        const net = typeof netRaw === 'number' ? netRaw : Number(netRaw ?? NaN);
        const fee = Number.isFinite(feeDirect) ? feeDirect : Number.isFinite(gross) && Number.isFinite(net) ? gross - net : NaN;
        value = Number.isFinite(gross) && gross !== 0 && Number.isFinite(fee) ? Math.abs((fee / gross) * 100) : NaN;
      }

      if (!Number.isFinite(value)) {
        continue;
      }

      const key = `${acquirer}|${method}`;
      const current = buckets.get(key) ?? { acquirer, method, sum: 0, count: 0 };
      current.sum += value;
      current.count += 1;
      buckets.set(key, current);
    }

    const sortedAcquirers = [
      ...byAcquirer.map((entry) => entry.name),
      ...allAcquirers.filter((name) => !byAcquirer.some((entry) => entry.name === name)),
    ];

    const matrix: Array<{
      acquirer: string;
      method: string;
      avg: number | null;
      count: number;
    }> = [];
    for (const acquirer of sortedAcquirers) {
      for (const method of methods) {
        const key = `${acquirer}|${method}`;
        const item = buckets.get(key);
        matrix.push({
          acquirer,
          method,
          avg: item && item.count ? item.sum / item.count : null,
          count: item?.count ?? 0,
        });
      }
    }

    return matrix
      .sort((a, b) => {
        const acqDiff =
          sortedAcquirers.indexOf(a.acquirer) - sortedAcquirers.indexOf(b.acquirer);
        if (acqDiff !== 0) {
          return acqDiff;
        }
        return methods.indexOf(a.method as (typeof methods)[number]) -
          methods.indexOf(b.method as (typeof methods)[number]);
      });
  }, [financialRows, byAcquirer]);

  const filteredFinancialRowsByTaxCard = useMemo(() => {
    if (!selectedTaxCard) {
      return financialRows;
    }
    const normalizeMethod = (value: unknown) => {
      const raw = String(value ?? '').trim().toUpperCase();
      if (!raw) {
        return '';
      }
      if (raw.includes('DEBIT')) {
        return 'DEBITO';
      }
      if (raw.includes('CRED')) {
        return 'CREDITO';
      }
      if (raw.includes('VOUCHER')) {
        return 'VOUCHER';
      }
      if (raw.includes('PIX')) {
        return 'PIX';
      }
      return raw;
    };
    return financialRows.filter((row) => {
      const acquirer = String(row.ACQUIRER ?? row.ACQ_PROVIDER ?? 'N/A').toUpperCase();
      const method = normalizeMethod(row.ACQ_CANON_METHOD_RESOLVED ?? row.CANON_METHOD);
      return acquirer === selectedTaxCard.acquirer && method === selectedTaxCard.method;
    });
  }, [financialRows, selectedTaxCard]);

  const totalTableItems = filteredFinancialRowsByTaxCard.length;
  const totalTablePages = Math.max(1, Math.ceil(totalTableItems / tablePageSize));
  const paginatedFinancialRows = useMemo(() => {
    const start = (tablePage - 1) * tablePageSize;
    return filteredFinancialRowsByTaxCard.slice(start, start + tablePageSize);
  }, [filteredFinancialRowsByTaxCard, tablePage, tablePageSize]);

  // Top filters (date/acquirer) are the source of truth.
  // Any card-level subfilter must reset when top filters change.
  useEffect(() => {
    setSelectedTaxCard(null);
    setTablePage(1);
  }, [dateFrom, dateTo, acquirerFilter]);

  useEffect(() => {
    setTablePage(1);
  }, [selectedTaxCard]);

  useEffect(() => {
    if (tablePage > totalTablePages) {
      setTablePage(totalTablePages);
    }
  }, [tablePage, totalTablePages]);

  // If the selected card no longer exists in the current dataset, reset it.
  useEffect(() => {
    if (!selectedTaxCard) {
      return;
    }
    const exists = avgFeesByAcquirerMethod.some(
      (item) =>
        item.acquirer === selectedTaxCard.acquirer &&
        item.method === selectedTaxCard.method &&
        item.count > 0,
    );
    if (!exists) {
      setSelectedTaxCard(null);
    }
  }, [avgFeesByAcquirerMethod, selectedTaxCard]);

  return (
    <div className="flex flex-col gap-6">
      <header className="mb-1 flex flex-col gap-3">
        <div>
          <h1 className="text-xl font-semibold">Painel Financeiro das Adquirentes</h1>
          <p className="text-xs text-slate-500">
            Dados financeiros de Cielo, Sipag e Sicredi (conciliados e nao conciliados), de {dateFrom} ate {dateTo}
            {isFetching ? ' • Atualizando...' : ''}
          </p>
        </div>

        <Card className="border border-slate-200 p-3">
          <div className="flex w-full min-w-0 flex-wrap items-end gap-3">
            <div className="flex flex-wrap overflow-hidden rounded-md border">
              <button
                type="button"
                onClick={() => setRangePreset('today')}
                className={`px-3 py-1 text-sm ${rangePreset === 'today' ? 'bg-slate-900 text-white' : 'bg-white hover:bg-slate-50'}`}
              >
                Hoje
              </button>
              <button
                type="button"
                onClick={() => setRangePreset('yesterday')}
                className={`px-3 py-1 text-sm ${rangePreset === 'yesterday' ? 'bg-slate-900 text-white' : 'bg-white hover:bg-slate-50'}`}
              >
                Ontem
              </button>
              <button
                type="button"
                onClick={() => setRangePreset('7d')}
                className={`px-3 py-1 text-sm ${rangePreset === '7d' ? 'bg-slate-900 text-white' : 'bg-white hover:bg-slate-50'}`}
              >
                7 dias
              </button>
              <button
                type="button"
                onClick={() => setRangePreset('30d')}
                className={`px-3 py-1 text-sm ${rangePreset === '30d' ? 'bg-slate-900 text-white' : 'bg-white hover:bg-slate-50'}`}
              >
                30 dias
              </button>
            </div>

            <div className="flex items-center gap-2">
              <label className="text-xs font-semibold uppercase text-slate-500">De</label>
              <input
                type="date"
                className="rounded-md border px-2 py-1 text-sm"
                value={dateFrom}
                onChange={(event) => {
                  setRangePreset('custom');
                  setDateFrom(event.target.value);
                }}
              />
            </div>
            <div className="flex items-center gap-2">
              <label className="text-xs font-semibold uppercase text-slate-500">Ate</label>
              <input
                type="date"
                className="rounded-md border px-2 py-1 text-sm"
                value={dateTo}
                onChange={(event) => {
                  setRangePreset('custom');
                  setDateTo(event.target.value);
                }}
              />
            </div>
            <div className="flex items-center gap-2">
              <label className="text-xs font-semibold uppercase text-slate-500">Adquirente</label>
              <select
                className="rounded-md border px-2 py-1 text-sm"
                value={acquirerFilter}
                onChange={(event) => setAcquirerFilter(event.target.value as AcquirerFilter)}
              >
                <option value="ALL">Todas</option>
                <option value="CIELO">Cielo</option>
                <option value="SIPAG">Sipag</option>
                <option value="SICREDI">Sicredi</option>
              </select>
            </div>
            <div className="sm:ml-auto">
              <button
                type="button"
                onClick={() => refetch()}
                className="rounded-md border px-3 py-1 text-sm hover:bg-slate-50"
                disabled={isFetching}
                title={isFetching ? 'Atualizando...' : 'Recarregar'}
              >
                Recarregar
              </button>
            </div>
          </div>
        </Card>
      </header>

      {isLoading && (
        <Card className="p-4">
          <p className="text-sm text-slate-600">Carregando dados financeiros das adquirentes...</p>
        </Card>
      )}

      {isError && (
        <Card className="p-4">
          <p className="text-sm text-red-600">Erro ao carregar dados financeiros das adquirentes.</p>
        </Card>
      )}

      {!isLoading && !isError && financialRows.length === 0 && (
        <Card className="p-4">
          <p className="text-sm text-slate-600">Sem dados financeiros para os filtros atuais.</p>
        </Card>
      )}

      {!isLoading && !isError && financialRows.length > 0 && (
        <>
          <section className="grid gap-4 lg:grid-cols-7">
            <Card className="p-4 lg:col-span-2">
              <div className="mb-3 text-xs uppercase text-slate-500">Distribuicao por Adquirente</div>
              <div className="space-y-2">
                {byAcquirer.map((entry) => (
                  <div key={entry.name}>
                    <div className="mb-1 flex justify-between text-sm">
                      <span className="font-medium text-slate-700">
                        <AcquirerLogo acquirer={entry.name} className="h-5" />
                      </span>
                      <span className="text-slate-500">
                        {entry.value} ({entry.pct.toFixed(1)}%)
                      </span>
                    </div>
                    <div className="h-2 w-full rounded bg-slate-100">
                      <div
                        className="h-2 rounded bg-slate-700"
                        style={{ width: `${Math.max(2, entry.pct)}%` }}
                      />
                    </div>
                  </div>
                ))}
              </div>
            </Card>

            <div className="lg:col-span-3">
              <InsightsBar
                rows={financialCanonRows}
                financialRows={filteredFinancialRowsByTaxCard}
                comparisonRows={financialRows}
                showBrandAnalysisCard={false}
                showComparisonCard
                showTopBrandsCard
              />
            </div>

            <Card className="p-4 lg:col-span-2">
              <div className="grid gap-3 sm:grid-cols-2">
                <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
                  <div className="text-xs uppercase text-slate-500">Transacoes</div>
                  <div className="text-[0.8rem] font-bold">{financialRows.length}</div>
                </div>
                <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
                  <div className="text-xs uppercase text-slate-500">Taxa Total</div>
                  <div className="text-[0.8rem] font-bold">{formatMoneyBRL(finance.fee)}</div>
                  <div className="mt-1 text-xs text-slate-500">{finance.feePerc.toFixed(2)}% do bruto</div>
                </div>
                <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
                  <div className="text-xs uppercase text-slate-500">Valor Bruto</div>
                  <div className="text-[0.8rem] font-bold">{formatMoneyBRL(finance.gross)}</div>
                </div>
                <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
                  <div className="text-xs uppercase text-slate-500">Valor Liquido</div>
                  <div className="text-[0.8rem] font-bold">{formatMoneyBRL(finance.net)}</div>
                </div>
              </div>
            </Card>
          </section>

          <section className="grid gap-4 lg:grid-cols-4">
            <Card className="p-4 lg:col-span-3">
              <div className="mb-3 text-xs uppercase text-slate-500">Taxas (media %)</div>
              {avgFeesByAcquirerMethod.length === 0 ? (
                <div className="text-sm text-slate-500">Sem dados de taxa no periodo.</div>
              ) : (
                <div className="grid gap-2 md:grid-cols-4">
                  {avgFeesByAcquirerMethod.map((item) => (
                    (() => {
                      const isSelected =
                        selectedTaxCard?.acquirer === item.acquirer &&
                        selectedTaxCard?.method === item.method;
                      return (
                    <div
                      key={`${item.acquirer}-${item.method}`}
                      className={`cursor-pointer rounded border px-3 py-2 transition ${
                        isSelected
                          ? 'border-slate-700 bg-slate-100 ring-1 ring-slate-300'
                          : 'border-slate-200 bg-slate-50 hover:bg-slate-100'
                      }`}
                      onClick={() =>
                        setSelectedTaxCard((current) =>
                          current &&
                          current.acquirer === item.acquirer &&
                          current.method === item.method
                            ? null
                            : { acquirer: item.acquirer, method: item.method },
                        )
                      }
                      title="Clique para ver os itens deste grupo na tabela abaixo"
                    >
                      <div className="flex items-center gap-2 text-xs font-semibold uppercase text-slate-500">
                        <AcquirerLogo acquirer={item.acquirer} className="h-4" />
                        <span>{item.method}</span>
                      </div>
                      <div className="text-lg font-semibold text-slate-800">
                        {item.avg === null ? '--' : `${item.avg.toFixed(2)}%`}
                      </div>
                      <div className="text-[11px] text-slate-500">
                        amostra: {item.count}
                      </div>
                    </div>
                      );
                    })()
                  ))}
                </div>
              )}
            </Card>

            <div className="lg:col-span-1 h-full">
              <InsightsBar
                rows={financialCanonRows}
                financialRows={filteredFinancialRowsByTaxCard}
                comparisonRows={financialRows}
                showBrandAnalysisCard
                showComparisonCard={false}
                showTopBrandsCard={false}
                brandAnalysisVertical
                fillHeight
              />
            </div>
          </section>

          <Card className="p-6">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <h2 className="text-lg font-semibold text-slate-800">
                {selectedTaxCard
                  ? `Itens do card: ${selectedTaxCard.acquirer} ${selectedTaxCard.method}`
                  : 'Atividade recente'}
              </h2>
              {selectedTaxCard ? (
                <button
                  type="button"
                  onClick={() => setSelectedTaxCard(null)}
                  className="rounded-md border px-2 py-1 text-xs hover:bg-slate-50"
                >
                  Limpar filtro do card
                </button>
              ) : null}
            </div>
            {filteredFinancialRowsByTaxCard.length === 0 ? (
              <p className="mt-2 text-sm text-slate-500">Sem itens para o card selecionado.</p>
            ) : null}
            <div className="mt-4 overflow-x-auto">
              <table className="min-w-full text-left text-sm">
                <thead className="text-xs uppercase text-slate-500">
                  <tr>
                    <th className="px-2 py-2">Data/Hora</th>
                    <th className="px-2 py-2">Adquirente</th>
                    <th className="px-2 py-2">Valor Bruto</th>
                    <th className="px-2 py-2">%Taxa</th>
                    <th className="px-2 py-2">Valor Liquido</th>
                    <th className="px-2 py-2">Metodo</th>
                    <th className="px-2 py-2">Tipo</th>
                    <th className="px-2 py-2">Bandeira</th>
                  </tr>
                </thead>
                <tbody>
                  {paginatedFinancialRows.map((row, index) => {
                    const grossRaw = row.ACQ_GROSS_AMOUNT ?? row.GROSS_AMOUNT;
                    const netRaw = row.ACQ_NET_AMOUNT ?? row.NET_AMOUNT;
                    const feeRaw = row.ACQ_FEE_AMOUNT;
                    const percRaw = row.ACQ_PERC_TAXA;
                    const gross =
                      typeof grossRaw === 'number' ? grossRaw : grossRaw ? Number(grossRaw) : null;
                    const net =
                      typeof netRaw === 'number' ? netRaw : netRaw ? Number(netRaw) : null;
                    let perc =
                      typeof percRaw === 'number'
                        ? percRaw
                        : percRaw
                          ? Number(percRaw)
                          : null;
                    if ((perc === null || Number.isNaN(perc)) && gross && gross !== 0) {
                      const fee =
                        typeof feeRaw === 'number'
                          ? feeRaw
                          : feeRaw
                            ? Number(feeRaw)
                            : net !== null
                              ? gross - net
                              : null;
                      if (fee !== null && Number.isFinite(fee)) {
                        perc = (fee / gross) * 100;
                      }
                    }
                    const provider = String(row.ACQUIRER ?? row.ACQ_PROVIDER ?? '-');
                    const methodGroupRaw = String(
                      row.ACQ_CANON_METHOD_GROUP_RESOLVED ?? row.CANON_METHOD_GROUP ?? '',
                    ).toUpperCase();
                    const method = methodGroupRaw.includes('PIX') ? 'PIX' : 'CARTÃO';
                    const typeRaw = String(
                      row.ACQ_CANON_METHOD_RESOLVED ?? row.CANON_METHOD ?? '',
                    ).toUpperCase();
                    const type = typeRaw.includes('PIX')
                      ? 'Pix'
                      : typeRaw.includes('VOUCHER')
                        ? 'Voucher'
                        : typeRaw.includes('DEBIT')
                          ? 'Debito'
                          : typeRaw.includes('CRED')
                            ? 'Credito'
                            : '-';
                    const brand = String(
                      row.ACQ_CANON_BRAND_RESOLVED ?? row.ACQ_BRAND_RESOLVED ?? row.BRAND ?? '-',
                    );
                    return (
                      <tr
                        key={`${String(row.ID ?? row.INTERDATA_ID ?? index)}-${index}`}
                        className="border-t"
                      >
                        <td className="px-2 py-2 text-slate-700">
                          {formatDatetimeCompact(row.SALE_DATETIME ?? row.CREATED_AT ?? row.MATCHED_AT)}
                        </td>
                        <td className="px-2 py-2 text-slate-700">
                          <AcquirerLogo acquirer={provider} className="h-5" />
                        </td>
                        <td className="px-2 py-2 text-slate-700">{formatMoneyBRL(gross)}</td>
                        <td className="px-2 py-2 font-semibold text-red-600">
                          {perc === null || Number.isNaN(perc) ? '--' : `${Math.abs(perc).toFixed(2)}%`}
                        </td>
                        <td className="px-2 py-2 text-slate-700">{formatMoneyBRL(net)}</td>
                        <td className="px-2 py-2 text-slate-700">{method}</td>
                        <td className="px-2 py-2 text-slate-700">{type}</td>
                        <td className="px-2 py-2 text-slate-700">{brand}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            <div className="mt-4 flex flex-wrap items-center justify-between gap-3 text-sm text-slate-600">
              <div>
                Mostrando{' '}
                {totalTableItems === 0 ? 0 : (tablePage - 1) * tablePageSize + 1}
                {' - '}
                {Math.min(tablePage * tablePageSize, totalTableItems)}
                {' de '}
                {totalTableItems}
              </div>
              <div className="flex items-center gap-2">
                <label className="text-xs uppercase text-slate-500">Itens/pag.</label>
                <select
                  className="rounded-md border px-2 py-1 text-sm"
                  value={tablePageSize}
                  onChange={(event) => {
                    setTablePageSize(Number(event.target.value));
                    setTablePage(1);
                  }}
                >
                  <option value={25}>25</option>
                  <option value={50}>50</option>
                  <option value={100}>100</option>
                  <option value={200}>200</option>
                </select>
                <button
                  type="button"
                  className="rounded-md border px-2 py-1 disabled:cursor-not-allowed disabled:opacity-50"
                  onClick={() => setTablePage((current) => Math.max(1, current - 1))}
                  disabled={tablePage <= 1}
                >
                  Anterior
                </button>
                <span>
                  Pagina {tablePage} de {totalTablePages}
                </span>
                <button
                  type="button"
                  className="rounded-md border px-2 py-1 disabled:cursor-not-allowed disabled:opacity-50"
                  onClick={() => setTablePage((current) => Math.min(totalTablePages, current + 1))}
                  disabled={tablePage >= totalTablePages}
                >
                  Proxima
                </button>
              </div>
            </div>
          </Card>
        </>
      )}
    </div>
  );
};
