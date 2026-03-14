import { useEffect, useMemo, useRef, useState } from 'react';
import { useMutation, useQuery } from '@tanstack/react-query';
import { fetchInterdataSales, triggerInterdataReconciliation } from '../api/interdata';
import { fetchAcquirerSales, fetchAcquirerSalesUnified } from '../api/acquirerImport';
import { StandardTable } from '../components/StandardTable';
import { ReconciliationFilters, ReconciliationFiltersValues } from '../components/Filters/ReconciliationFilters';
import { Card } from '../components/common/Card';
import { Button } from '../components/common/Button';
import { Input } from '../components/common/Input';
import { Select } from '../components/common/Select';
import { useSearchParams } from 'react-router-dom';
import { fetchReconciliations, fetchReconciliationDetails, reconcileManual } from '../api/reconciliation';
import {
  formatDatetimeCompact,
  formatMoneyBRL,
  normalizeStandardRow,
  toTextAny,
} from '../modules/reconciliation/normalizeRow';
import { StandardRow } from '../modules/reconciliation/types';
import { AcquirerUnifiedSale, InterdataSale, ReconciliationDetails } from '../api/types';
import { dlog, derr } from '../lib/debug';
import styles from './Reconciliation.module.css';

type NormalizedUiRow = StandardRow & {
  rowId?: number;
  raw?: Record<string, unknown> | AcquirerUnifiedSale;
};

const defaultFilters: ReconciliationFiltersValues = {
  dateFrom: '',
  dateTo: '',
  status: '',
  search: '',
  paymentType: '',
  brand: '',
};

export const Reconciliation = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [draftFilters, setDraftFilters] = useState<ReconciliationFiltersValues>(defaultFilters);
  const [dateError, setDateError] = useState('');
  const [page, setPage] = useState(1);
  const limit = 100;
  const isDev = import.meta.env.DEV;
  const [selected, setSelected] = useState<StandardRow | null>(null);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [detailsById, setDetailsById] = useState<Record<number, ReconciliationDetails>>({});
  const [detailsLoadingById, setDetailsLoadingById] = useState<Record<number, boolean>>({});
  const [detailsErrorById, setDetailsErrorById] = useState<Record<number, string>>({});
  const [viewMode, setViewMode] = useState<'pending' | 'reconciled'>('pending');
  const [pendingScope, setPendingScope] = useState<'INTERDATA' | 'CIELO' | 'SIPAG' | 'SICREDI'>(
    'INTERDATA',
  );
  const [manualMode, setManualMode] = useState(false);
  const [manualAcquirerFilter, setManualAcquirerFilter] = useState<
    'ALL' | 'CIELO' | 'SIPAG' | 'SICREDI'
  >('ALL');
  const [manualSearch, setManualSearch] = useState('');
  const [manualReason, setManualReason] = useState<
    '' | 'TIME_DIFF' | 'NSU_DIFF' | 'BATCH' | 'OTHER'
  >('');
  const [manualNotes, setManualNotes] = useState('');
  const [selectedInterdata, setSelectedInterdata] = useState<InterdataSale | null>(null);
  const [selectedAcquirer, setSelectedAcquirer] = useState<AcquirerUnifiedSale | null>(null);
  const [previewRows, setPreviewRows] = useState<Array<Record<string, unknown>>>([]);
  const [previewActive, setPreviewActive] = useState(false);
  const [manualPendingPage, setManualPendingPage] = useState(1);
  const [manualAcquirerPage, setManualAcquirerPage] = useState(1);
  const [pendingSort, setPendingSort] = useState<{ by: 'datetime' | 'amount' | null; dir: 'asc' | 'desc' | null }>({
    by: null,
    dir: null,
  });
  const [cieloSort, setCieloSort] = useState<{ by: 'datetime' | 'amount' | null; dir: 'asc' | 'desc' | null }>({
    by: null,
    dir: null,
  });
  const [sipagSort, setSipagSort] = useState<{ by: 'datetime' | 'amount' | null; dir: 'asc' | 'desc' | null }>({
    by: null,
    dir: null,
  });
  const [reconciledSort, setReconciledSort] = useState<{ by: 'datetime' | 'amount' | null; dir: 'asc' | 'desc' | null }>({
    by: null,
    dir: null,
  });
  const [autoReconWaiting, setAutoReconWaiting] = useState(false);
  const autoReconRetryTimerRef = useRef<number | null>(null);
  const [manualLeftSort, setManualLeftSort] = useState<{ by: 'datetime' | 'amount' | null; dir: 'asc' | 'desc' | null }>({
    by: null,
    dir: null,
  });
  const [manualRightSort, setManualRightSort] = useState<{ by: 'datetime' | 'amount' | null; dir: 'asc' | 'desc' | null }>({
    by: null,
    dir: null,
  });

  const appliedFilters = useMemo<ReconciliationFiltersValues>(() => {
    return {
      dateFrom: searchParams.get('dateFrom') ?? '',
      dateTo: searchParams.get('dateTo') ?? '',
      status: searchParams.get('status') ?? '',
      search: searchParams.get('search') ?? '',
      paymentType: searchParams.get('paymentType') ?? '',
      brand: searchParams.get('brand') ?? '',
    };
  }, [searchParams]);

  useEffect(() => {
    const hasDateFrom = Boolean(searchParams.get('dateFrom'));
    const hasDateTo = Boolean(searchParams.get('dateTo'));
    if (hasDateFrom || hasDateTo) {
      return;
    }
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const yyyy = yesterday.getFullYear();
    const mm = String(yesterday.getMonth() + 1).padStart(2, '0');
    const dd = String(yesterday.getDate()).padStart(2, '0');
    const dMinusOne = `${yyyy}-${mm}-${dd}`;

    const next = new URLSearchParams(searchParams);
    next.set('dateFrom', dMinusOne);
    next.set('dateTo', dMinusOne);
    setSearchParams(next, { replace: true });
  }, [searchParams, setSearchParams]);

  useEffect(() => {
    setDraftFilters({
      ...appliedFilters,
      dateFrom: normalizeDate(appliedFilters.dateFrom),
      dateTo: normalizeDate(appliedFilters.dateTo),
    });
    setDateError('');
  }, [appliedFilters]);

  useEffect(() => {
    if (manualMode) {
      setSelected(null);
    }
  }, [manualMode]);

  useEffect(() => {
    if (manualMode && viewMode !== 'pending') {
      setManualMode(false);
    }
  }, [manualMode, viewMode]);

  useEffect(() => {
    setPage(1);
    setSelected(null);
    setSelectedId(null);
    setExpandedId(null);
  }, [pendingScope]);

  useEffect(() => {
    if (!manualMode) {
      setManualSearch('');
      setManualReason('');
      setManualNotes('');
      setSelectedInterdata(null);
      setSelectedAcquirer(null);
      setManualAcquirerFilter('ALL');
      setManualPendingPage(1);
      setManualAcquirerPage(1);
    }
  }, [manualMode]);

  useEffect(() => {
    setSelectedAcquirer(null);
    setManualAcquirerPage(1);
  }, [manualAcquirerFilter]);

  useEffect(() => {
    const previousBodyOverflow = document.body.style.overflow;
    const previousHtmlOverflow = document.documentElement.style.overflow;
    document.body.style.overflow = 'hidden';
    document.documentElement.style.overflow = 'hidden';
    return () => {
      document.body.style.overflow = previousBodyOverflow;
      document.documentElement.style.overflow = previousHtmlOverflow;
    };
  }, []);

  const normalizeDate = (value: string) => {
    const text = value.trim();
    if (!text) {
      return '';
    }
    const pad = (num: number) => String(num).padStart(2, '0');
    const isValidDateParts = (year: number, month: number, day: number) => {
      if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
        return false;
      }
      if (month < 1 || month > 12 || day < 1 || day > 31) {
        return false;
      }
      const date = new Date(year, month - 1, day);
      return (
        date.getFullYear() === year &&
        date.getMonth() === month - 1 &&
        date.getDate() === day
      );
    };
    const brMatch = text.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (brMatch) {
      const day = Number(brMatch[1]);
      const month = Number(brMatch[2]);
      const year = Number(brMatch[3]);
      if (!isValidDateParts(year, month, day)) {
        return '';
      }
      return `${year}-${pad(month)}-${pad(day)}`;
    }
    const isoMatch = text.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
    if (isoMatch) {
      const year = Number(isoMatch[1]);
      const month = Number(isoMatch[2]);
      const day = Number(isoMatch[3]);
      if (!isValidDateParts(year, month, day)) {
        return '';
      }
      return `${year}-${pad(month)}-${pad(day)}`;
    }
    return '';
  };

  const parseDateSafe = (value?: string | Date | null) => {
    if (!value) {
      return null;
    }
    const date = value instanceof Date ? value : new Date(String(value));
    return Number.isNaN(date.getTime()) ? null : date;
  };

  const formatDateOnly = (value?: string | Date | null) => {
    const date = parseDateSafe(value);
    if (!date) {
      return '';
    }
    const pad = (num: number) => String(num).padStart(2, '0');
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
  };

  const normalizedAppliedDateFrom = normalizeDate(appliedFilters.dateFrom);
  const normalizedAppliedDateTo = normalizeDate(appliedFilters.dateTo);
  const todayDate = useMemo(() => {
    const now = new Date();
    const yyyy = now.getFullYear();
    const mm = String(now.getMonth() + 1).padStart(2, '0');
    const dd = String(now.getDate()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd}`;
  }, []);
  const autoReconciliationLabel = normalizedAppliedDateFrom
    ? `Conciliar de ${normalizedAppliedDateFrom} até hoje`
    : 'Conciliar pendentes até hoje';
  const autoReconciliationTitle = normalizedAppliedDateFrom
    ? `Executa a conciliação automática de ${normalizedAppliedDateFrom} até ${todayDate}, com limite de 10000 pendências.`
    : `Executa a conciliação automática do início dos pendentes até ${todayDate}, com limite de 10000 pendências.`;
  const normalizePaymentFilterValue = (value?: string) => {
    const raw = toTextAny(value)
      .trim()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toUpperCase();
    if (!raw) {
      return '';
    }
    if (raw.includes('CARTAO') || raw === 'CARD') {
      return 'CARD';
    }
    if (raw.includes('PIX')) {
      return 'PIX';
    }
    if (raw.includes('DEBITO') || raw.includes('DEBIT')) {
      return 'DEBIT';
    }
    if (raw.includes('CREDITO') || raw.includes('CREDIT') || raw.includes('CRED')) {
      return 'CREDIT';
    }
    return raw;
  };
  const normalizeAsciiUpper = (value: unknown) =>
    toTextAny(value)
      .trim()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toUpperCase();
  const normalizeIdentifierText = (value: unknown) =>
    toTextAny(value)
      .trim()
      .toUpperCase()
      .replace(/[^A-Z0-9]/g, '');
  const includesCanon = (value: unknown, needle?: string) => {
    const left = normalizeAsciiUpper(value);
    const right = normalizeAsciiUpper(needle);
    if (!right) {
      return true;
    }
    return left.includes(right);
  };
  const backendPaymentTypeFilter = normalizePaymentFilterValue(appliedFilters.paymentType);
  const draftPaymentTypeFilter = normalizePaymentFilterValue(draftFilters.paymentType);
  const isDraftPaymentPix = draftPaymentTypeFilter === 'PIX';

  useEffect(() => {
    if (isDraftPaymentPix && (draftFilters.brand ?? '')) {
      setDraftFilters((current) => ({ ...current, brand: '' }));
    }
  }, [isDraftPaymentPix, draftFilters.brand]);

  function formatDateInput(value: string) {
    if (!value) {
      return '';
    }
    const pad = (num: number) => String(num).padStart(2, '0');
    const isoMatch = value.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
    if (isoMatch) {
      const year = isoMatch[1];
      const month = pad(Number(isoMatch[2]));
      const day = pad(Number(isoMatch[3]));
      return `${day}/${month}/${year}`;
    }
    const brMatch = value.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (brMatch) {
      const day = pad(Number(brMatch[1]));
      const month = pad(Number(brMatch[2]));
      const year = brMatch[3];
      return `${day}/${month}/${year}`;
    }
    return value;
  }

  const dataSource =
    viewMode === 'reconciled' ? 'RECONCILIATION' : pendingScope;

  const buildSearchParams = (filters: ReconciliationFiltersValues) => {
    const next = new URLSearchParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value) {
        next.set(key, value);
      }
    });
    return next;
  };

  const query = useQuery({
    queryKey: [
      'reconciliationSales',
      viewMode,
      dataSource,
      viewMode === 'reconciled' && isDev ? 1 : page,
      viewMode === 'reconciled' && isDev ? 0 : limit,
      normalizedAppliedDateFrom,
      normalizedAppliedDateTo,
      appliedFilters.status,
      appliedFilters.search,
      backendPaymentTypeFilter,
      appliedFilters.brand,
      viewMode === 'pending' ? pendingSort.by : reconciledSort.by,
      viewMode === 'pending' ? pendingSort.dir : reconciledSort.dir,
    ],
    queryFn: () =>
      viewMode === 'reconciled'
        ? fetchReconciliations(
            isDev
              ? {
                  dateFrom: normalizedAppliedDateFrom || undefined,
                  dateTo: normalizedAppliedDateTo || undefined,
                  status: appliedFilters.status || undefined,
                  search: appliedFilters.search || undefined,
                  paymentType: backendPaymentTypeFilter || undefined,
                  brand: appliedFilters.brand || undefined,
                  sortBy: reconciledSort.by ?? undefined,
                  sortDir: reconciledSort.dir ?? undefined,
                }
              : {
                  dateFrom: normalizedAppliedDateFrom || undefined,
                  dateTo: normalizedAppliedDateTo || undefined,
                  status: appliedFilters.status || undefined,
                  search: appliedFilters.search || undefined,
                  paymentType: backendPaymentTypeFilter || undefined,
                  brand: appliedFilters.brand || undefined,
                  page,
                  limit,
                  sortBy: reconciledSort.by ?? undefined,
                  sortDir: reconciledSort.dir ?? undefined,
                },
          )
        : dataSource === 'CIELO'
        ? fetchAcquirerSales({
            acquirer: 'cielo',
            dateFrom: normalizedAppliedDateFrom || undefined,
            dateTo: normalizedAppliedDateTo || undefined,
            page,
            limit,
            status: appliedFilters.status || undefined,
            brand: appliedFilters.brand || undefined,
            search: appliedFilters.search || undefined,
            paymentType: backendPaymentTypeFilter || undefined,
            sortBy: pendingSort.by ?? undefined,
            sortDir: pendingSort.dir ?? undefined,
          })
        : dataSource === 'SIPAG'
          ? fetchAcquirerSales({
              acquirer: 'sipag',
              dateFrom: normalizedAppliedDateFrom || undefined,
              dateTo: normalizedAppliedDateTo || undefined,
              page,
              limit,
              status: appliedFilters.status || undefined,
              brand: appliedFilters.brand || undefined,
              search: appliedFilters.search || undefined,
              paymentType: backendPaymentTypeFilter || undefined,
              sortBy: pendingSort.by ?? undefined,
              sortDir: pendingSort.dir ?? undefined,
            })
          : dataSource === 'SICREDI'
            ? fetchAcquirerSales({
                acquirer: 'sicredi',
                dateFrom: normalizedAppliedDateFrom || undefined,
                dateTo: normalizedAppliedDateTo || undefined,
                page,
                limit,
                status: appliedFilters.status || undefined,
                brand: appliedFilters.brand || undefined,
                search: appliedFilters.search || undefined,
                paymentType: backendPaymentTypeFilter || undefined,
                sortBy: pendingSort.by ?? undefined,
                sortDir: pendingSort.dir ?? undefined,
              })
          : fetchInterdataSales({
              dateFrom: normalizedAppliedDateFrom || undefined,
              dateTo: normalizedAppliedDateTo || undefined,
              page,
              limit,
              status: appliedFilters.status || undefined,
              search: appliedFilters.search || undefined,
              paymentType: backendPaymentTypeFilter || undefined,
              brand: appliedFilters.brand || undefined,
              sortBy: pendingSort.by ?? undefined,
              sortDir: pendingSort.dir ?? undefined,
            }),
    placeholderData: (previous) => previous,
    enabled: !manualMode,
  });

  const reconciliationTrigger = useMutation({
    mutationFn: (params: { dateFrom?: string; dateTo?: string }) =>
      triggerInterdataReconciliation(params),
    onSuccess: (data) => {
      const ran = Boolean((data as any)?.ran);
      const reason = String((data as any)?.reason ?? '').trim().toLowerCase();
      if (!ran && reason === 'running') {
        setAutoReconWaiting(true);
        if (autoReconRetryTimerRef.current) {
          window.clearTimeout(autoReconRetryTimerRef.current);
        }
        autoReconRetryTimerRef.current = window.setTimeout(() => {
          reconciliationTrigger.mutate({
            dateFrom: normalizedAppliedDateFrom || undefined,
            dateTo: todayDate,
          });
        }, 3000);
        return;
      }

      if (autoReconRetryTimerRef.current) {
        window.clearTimeout(autoReconRetryTimerRef.current);
        autoReconRetryTimerRef.current = null;
      }
      setAutoReconWaiting(false);
      dlog('Conciliacao acionada', data);
      const preview = (data as any)?.data?.matchedPreview as Array<Record<string, unknown>> | undefined;
      if (preview && preview.length) {
        setPreviewRows(preview);
        setPreviewActive(true);
        setViewMode('reconciled');
        setPage(1);
      } else {
        setPreviewRows([]);
        setPreviewActive(false);
      }
      query.refetch();
    },
    onError: (error) => {
      if (autoReconRetryTimerRef.current) {
        window.clearTimeout(autoReconRetryTimerRef.current);
        autoReconRetryTimerRef.current = null;
      }
      setAutoReconWaiting(false);
      derr('Falha ao acionar conciliacao', error);
    },
  });

  useEffect(() => {
    return () => {
      if (autoReconRetryTimerRef.current) {
        window.clearTimeout(autoReconRetryTimerRef.current);
      }
    };
  }, []);

  const pendingQuery = useQuery({
    queryKey: [
      'manualInterdataSales',
      manualPendingPage,
      normalizedAppliedDateFrom,
      normalizedAppliedDateTo,
      appliedFilters.status,
      appliedFilters.search,
      backendPaymentTypeFilter,
      appliedFilters.brand,
      manualLeftSort.by,
      manualLeftSort.dir,
    ],
    queryFn: () =>
      fetchInterdataSales({
        dateFrom: normalizedAppliedDateFrom || undefined,
        dateTo: normalizedAppliedDateTo || undefined,
        page: manualPendingPage,
        limit,
        status: appliedFilters.status || undefined,
        search: appliedFilters.search || undefined,
        paymentType: backendPaymentTypeFilter || undefined,
        brand: appliedFilters.brand || undefined,
        sortBy: manualLeftSort.by ?? undefined,
        sortDir: manualLeftSort.dir ?? undefined,
      }),
    placeholderData: (previous) => previous,
    enabled: manualMode,
  });

  const suggestedDate = useMemo(
    () => parseDateSafe(selectedInterdata?.SALE_DATETIME ?? null),
    [selectedInterdata],
  );
  const suggestedDateFrom = suggestedDate ? formatDateOnly(suggestedDate) : '';
  const suggestedDateTo = suggestedDate ? formatDateOnly(suggestedDate) : '';
  const manualDateFrom = normalizedAppliedDateFrom || suggestedDateFrom;
  const manualDateTo = normalizedAppliedDateTo || suggestedDateTo;
  const manualBackendSearch = manualSearch || appliedFilters.search || '';

  const acquirerQuery = useQuery({
    queryKey: [
      'manualAcquirerSales',
      manualAcquirerFilter,
      manualAcquirerPage,
      manualDateFrom,
      manualDateTo,
      appliedFilters.status,
      appliedFilters.brand,
      appliedFilters.paymentType,
      appliedFilters.search,
      manualSearch,
      manualRightSort.by,
      manualRightSort.dir,
    ],
    queryFn: () =>
      fetchAcquirerSalesUnified({
        acquirers:
          manualAcquirerFilter === 'ALL'
            ? 'cielo,sipag,sicredi'
            : manualAcquirerFilter.toLowerCase(),
        dateFrom: manualDateFrom || undefined,
        dateTo: manualDateTo || undefined,
        page: manualAcquirerPage,
        limit,
        search: manualBackendSearch || undefined,
        sortBy: manualRightSort.by ?? undefined,
        sortDir: manualRightSort.dir ?? undefined,
      }),
    placeholderData: (previous) => previous,
    enabled: manualMode,
  });

  const manualMutation = useMutation({
    mutationFn: reconcileManual,
    onSuccess: () => {
      pendingQuery.refetch();
      acquirerQuery.refetch();
    },
  });

  const rows = (query.data as any)?.data ?? [];
  const pagedRows = useMemo(() => {
    if (!(isDev && viewMode === 'reconciled')) {
      return rows;
    }
    const start = (page - 1) * limit;
    return rows.slice(start, start + limit);
  }, [isDev, viewMode, rows, page, limit]);
  const previewNormalized = useMemo(
    () =>
      previewRows.map((row) => ({
        ID: row.interdataId,
        SALE_DATETIME: row.interdataDatetime,
        GROSS_AMOUNT: row.interdataAmount,
        CANON_METHOD_GROUP: row.interdataMethod,
        ACQUIRER: row.acquirer,
        ACQUIRER_ID: row.acquirerId,
        ACQ_SALE_DATETIME: row.acquirerDatetime,
        ACQ_GROSS_AMOUNT: row.acquirerGross,
        ACQ_NET_AMOUNT: row.acquirerNet,
        MATCH_REASON: row.matchReason,
      })),
    [previewRows],
  );
  const dataRows = previewActive && viewMode === 'reconciled' ? previewNormalized : pagedRows;
  const normalizedRows = useMemo<NormalizedUiRow[]>(() => {
    const source =
      viewMode === 'reconciled'
        ? 'reconciled'
        : pendingScope === 'INTERDATA'
          ? 'interdata'
          : pendingScope.toLowerCase();
    return dataRows.map((row: Record<string, unknown>) => ({
      ...normalizeStandardRow(row, source),
      rowId: typeof row.ID === 'number' ? row.ID : undefined,
      raw: row,
    }));
  }, [dataRows, viewMode, pendingScope]);

  const filterOptions = useMemo(() => {
    const toSortedUnique = (values: Array<string | null | undefined>) =>
      Array.from(
        new Set(
          values
            .map((value) => toTextAny(value).trim())
            .filter((value) => Boolean(value)),
        ),
      ).sort((a, b) => a.localeCompare(b, 'pt-BR'));

    const statusOptions = toSortedUnique(normalizedRows.map((row: NormalizedUiRow) => row.statusText));
    const paymentGroupFromRaw = normalizedRows.map((row: NormalizedUiRow) => {
      const raw = row.raw ?? {};
      return normalizeAsciiUpper(
        raw.ACQ_CANON_METHOD_GROUP_RESOLVED ??
          raw.CANON_METHOD_GROUP ??
          raw.interdataMethodGroup,
      );
    });
    const methodFromRaw = normalizedRows.map((row: NormalizedUiRow) => {
      const raw = row.raw ?? {};
      return normalizeAsciiUpper(
        raw.ACQ_CANON_METHOD_RESOLVED ??
          raw.CANON_METHOD ??
          raw.interdataMethod ??
          raw.ACQ_PAYMENT_METHOD_RESOLVED,
      );
    });

    const basePaymentOptions = ['CARD', 'PIX'];
    const paymentOptions = toSortedUnique([
      ...basePaymentOptions,
      ...paymentGroupFromRaw.filter((value: string) => value === 'CARD' || value === 'PIX'),
    ]);
    const normalizedMethodOptions = methodFromRaw
      .map((value: string) => {
        if (value === 'DEBIT') {
          return 'DEBITO';
        }
        if (value === 'CREDIT' || value === 'CREDITO_A_VISTA') {
          return 'CREDITO';
        }
        return value;
      })
      .filter((value: string) => ['DEBITO', 'CREDITO', 'VOUCHER', 'PIX'].includes(value));
    const baseBrandOptions = ['DEBITO', 'CREDITO', 'VOUCHER', 'PIX'];
    const brandOptions = toSortedUnique([...baseBrandOptions, ...normalizedMethodOptions]);

    const includeSelected = (options: string[], selected?: string) => {
      const value = toTextAny(selected).trim();
      if (!value || options.includes(value)) {
        return options;
      }
      return [value, ...options];
    };

    return {
      statusOptions: includeSelected(statusOptions, draftFilters.status),
      paymentOptions: includeSelected(paymentOptions, draftFilters.paymentType),
      brandOptions: includeSelected(brandOptions, draftFilters.brand),
    };
  }, [
    normalizedRows,
    draftFilters.status,
    draftFilters.paymentType,
    draftFilters.brand,
  ]);
  const brandOptionsForDraftPayment = useMemo(() => {
    if (isDraftPaymentPix) {
      return [];
    }
    return filterOptions.brandOptions.filter(
      (option) => normalizeAsciiUpper(option) !== 'PIX',
    );
  }, [filterOptions.brandOptions, isDraftPaymentPix]);

  const handleReconciledRowClick = async (row: StandardRow) => {
    const rowId = row.rowId;
    if (!rowId) {
      return;
    }
    if (expandedId === rowId) {
      setExpandedId(null);
      return;
    }
    setExpandedId(rowId);
    if (detailsById[rowId] || detailsLoadingById[rowId]) {
      return;
    }
    setDetailsLoadingById((current) => ({ ...current, [rowId]: true }));
    setDetailsErrorById((current) => {
      const next = { ...current };
      delete next[rowId];
      return next;
    });
    try {
      const response = await fetchReconciliationDetails(rowId);
      dlog('Reconciliation details payload', response);
      if (response?.data) {
        setDetailsById((current) => ({ ...current, [rowId]: response.data }));
      } else {
        setDetailsErrorById((current) => ({ ...current, [rowId]: 'Detalhes indisponiveis.' }));
      }
    } catch (error) {
      const message = (error as Error)?.message ?? 'Erro ao carregar detalhes.';
      setDetailsErrorById((current) => ({ ...current, [rowId]: message }));
    } finally {
      setDetailsLoadingById((current) => ({ ...current, [rowId]: false }));
    }
  };

  const totalsSummary = useMemo(() => {
    let grossTotal = 0;
    let netTotal = 0;
    let grossCount = 0;
    let netCount = 0;
    const statusCounts: Record<string, number> = {};

    normalizedRows.forEach((row: StandardRow) => {
      const raw = (row.raw ?? {}) as Record<string, unknown>;
      const grossValue =
        typeof raw.ACQ_GROSS_AMOUNT === 'number'
          ? raw.ACQ_GROSS_AMOUNT
          : typeof raw.GROSS_AMOUNT === 'number'
            ? raw.GROSS_AMOUNT
            : null;
      if (grossValue !== null && Number.isFinite(grossValue)) {
        grossTotal += grossValue;
        grossCount += 1;
      }
      const netValue =
        typeof raw.ACQ_NET_AMOUNT === 'number'
          ? raw.ACQ_NET_AMOUNT
          : typeof raw.NET_AMOUNT === 'number'
            ? raw.NET_AMOUNT
            : null;
      if (netValue !== null && Number.isFinite(netValue)) {
        netTotal += netValue;
        netCount += 1;
      }
      const statusKey = row.statusText
        ? row.statusText.trim().toUpperCase().replace(/\s+/g, '_')
        : '';
      if (statusKey) {
        statusCounts[statusKey] = (statusCounts[statusKey] ?? 0) + 1;
      }
    });

    return {
      gross: grossCount ? grossTotal : null,
      net: netCount ? netTotal : null,
      statusCounts,
    };
  }, [normalizedRows]);

  const currentSort = viewMode === 'pending' ? pendingSort : reconciledSort;

  const manualPendingRaw = (pendingQuery.data as any)?.data ?? [];
  const manualPendingFiltered = useMemo(() => {
    let data = manualPendingRaw as Array<Record<string, unknown>>;
    const acqAmount =
      typeof selectedAcquirer?.grossAmount === 'number' ? selectedAcquirer.grossAmount : null;
    const acqDate = parseDateSafe(selectedAcquirer?.saleDatetime ?? null);
    const acqIdentifiers = selectedAcquirer
      ? [
          normalizeIdentifierText(selectedAcquirer.nsu),
          normalizeIdentifierText(selectedAcquirer.authCode),
          normalizeIdentifierText(selectedAcquirer.terminal),
          normalizeIdentifierText(selectedAcquirer.pdv),
          normalizeIdentifierText((selectedAcquirer.raw as Record<string, unknown> | undefined)?.SALE_ID),
          normalizeIdentifierText((selectedAcquirer.raw as Record<string, unknown> | undefined)?.ORDER_NO),
          normalizeIdentifierText((selectedAcquirer.raw as Record<string, unknown> | undefined)?.YOUR_NUMBER),
          normalizeIdentifierText((selectedAcquirer.raw as Record<string, unknown> | undefined)?.TRANSACTION_NO),
        ].filter((value) => value.length >= 4)
      : [];

    if (!selectedAcquirer) {
      return data;
    }

    const matchedByIdentifier = data.filter((row) => {
      const rowIdentifiers = [
        normalizeIdentifierText(row.SALE_NO),
        normalizeIdentifierText(row.AUTH_NSU),
        normalizeIdentifierText(row.NSU),
        normalizeIdentifierText(row.AUTH_CODE),
      ].filter((value) => value.length >= 4);
      return rowIdentifiers.some((value) => acqIdentifiers.includes(value));
    });

    if (matchedByIdentifier.length) {
      data = matchedByIdentifier;
    } else if (acqAmount !== null) {
      const matchedByAmount = data.filter((row) => {
        const gross = typeof row.GROSS_AMOUNT === 'number' ? row.GROSS_AMOUNT : Number(row.GROSS_AMOUNT);
        return Number.isFinite(gross) && Math.abs(gross - acqAmount) <= 0.01;
      });
      if (matchedByAmount.length) {
        data = matchedByAmount;
      }
    }

    if (data.length && acqDate && !manualLeftSort.by) {
      data = [...data].sort((a, b) => {
        const left = parseDateSafe((a.SALE_DATETIME as string | Date | null) ?? null);
        const right = parseDateSafe((b.SALE_DATETIME as string | Date | null) ?? null);
        const leftDiff = left ? Math.abs(left.getTime() - acqDate.getTime()) : Number.MAX_SAFE_INTEGER;
        const rightDiff = right ? Math.abs(right.getTime() - acqDate.getTime()) : Number.MAX_SAFE_INTEGER;
        if (leftDiff !== rightDiff) {
          return leftDiff - rightDiff;
        }
        const leftAmount = typeof a.GROSS_AMOUNT === 'number' ? a.GROSS_AMOUNT : Number(a.GROSS_AMOUNT);
        const rightAmount = typeof b.GROSS_AMOUNT === 'number' ? b.GROSS_AMOUNT : Number(b.GROSS_AMOUNT);
        return Math.abs((Number.isFinite(leftAmount) ? leftAmount : 0) - (acqAmount ?? 0)) -
          Math.abs((Number.isFinite(rightAmount) ? rightAmount : 0) - (acqAmount ?? 0));
      });
    }

    return data;
  }, [manualPendingRaw, selectedAcquirer, manualLeftSort.by]);

  const manualPendingRows = useMemo<NormalizedUiRow[]>(
    () =>
      manualPendingFiltered.map((row: Record<string, unknown>) => ({
        ...normalizeStandardRow(row, 'interdata'),
        rowId: typeof row.ID === 'number' ? row.ID : undefined,
        raw: row,
      })),
    [manualPendingFiltered],
  );

  const manualAcquirerRaw = (acquirerQuery.data as any)?.data ?? [];
  const manualAcquirerFiltered = useMemo(() => {
    const includesInsensitive = (value: unknown, needle?: string) =>
      String(value ?? '').toUpperCase().includes(String(needle ?? '').toUpperCase());
    const baseData = manualAcquirerRaw as AcquirerUnifiedSale[];
    let data = baseData;
    const fromDate = normalizedAppliedDateFrom
      ? parseDateSafe(`${normalizedAppliedDateFrom}T00:00:00`)
      : null;
    const toDate = normalizedAppliedDateTo
      ? parseDateSafe(`${normalizedAppliedDateTo}T23:59:59`)
      : null;
    if (fromDate || toDate) {
      data = data.filter((row) => {
        const date = parseDateSafe(row.saleDatetime ?? null);
        if (!date) {
          return false;
        }
        if (fromDate && date.getTime() < fromDate.getTime()) {
          return false;
        }
        if (toDate && date.getTime() > toDate.getTime()) {
          return false;
        }
        return true;
      });
    }
    if (appliedFilters.status) {
      data = data.filter((row) => includesInsensitive(row.status, appliedFilters.status));
    }
    if (appliedFilters.brand) {
      data = data.filter((row) => {
        const raw = (row.raw ?? {}) as Record<string, unknown>;
        return (
          includesCanon(raw.CANON_METHOD, appliedFilters.brand) ||
          includesCanon(raw.CANON_BRAND, appliedFilters.brand) ||
          includesInsensitive(row.brand, appliedFilters.brand)
        );
      });
    }
    if (appliedFilters.paymentType) {
      data = data.filter((row) => {
        const raw = (row.raw ?? {}) as Record<string, unknown>;
        return (
          includesCanon(raw.CANON_METHOD_GROUP, appliedFilters.paymentType) ||
          includesCanon(raw.CANON_METHOD, appliedFilters.paymentType) ||
          includesInsensitive(raw.PAYMENT_METHOD, appliedFilters.paymentType) ||
          includesInsensitive(raw.CARD_TYPE, appliedFilters.paymentType) ||
          includesInsensitive(raw.PRODUCT, appliedFilters.paymentType)
        );
      });
    }
    if (appliedFilters.search) {
      data = data.filter((row) => {
        const raw = (row.raw ?? {}) as Record<string, unknown>;
        return (
          includesInsensitive(row.nsu, appliedFilters.search) ||
          includesInsensitive(row.authCode, appliedFilters.search) ||
          includesInsensitive(row.terminal, appliedFilters.search) ||
          includesInsensitive(row.pdv, appliedFilters.search) ||
          includesInsensitive(raw.SALE_ID, appliedFilters.search) ||
          includesInsensitive(raw.ORDER_NO, appliedFilters.search) ||
          includesInsensitive(raw.PAYMENT_CODE, appliedFilters.search)
        );
      });
    }
    const interAmount =
      typeof selectedInterdata?.GROSS_AMOUNT === 'number' ? selectedInterdata.GROSS_AMOUNT : null;
    if (interAmount !== null) {
      const matchedByAmount = data.filter((row) => Math.abs(row.grossAmount - interAmount) <= 0.01);
      if (matchedByAmount.length) {
        data = matchedByAmount;
      } else {
        data = baseData;
      }
    }

    const interDate = parseDateSafe(selectedInterdata?.SALE_DATETIME ?? null);
    if (interDate && !manualRightSort.by) {
      data = [...data].sort((a, b) => {
        const left = parseDateSafe(a.saleDatetime ?? null);
        const right = parseDateSafe(b.saleDatetime ?? null);
        const leftDiff = left ? Math.abs(left.getTime() - interDate.getTime()) : Number.MAX_SAFE_INTEGER;
        const rightDiff = right ? Math.abs(right.getTime() - interDate.getTime()) : Number.MAX_SAFE_INTEGER;
        if (leftDiff !== rightDiff) {
          return leftDiff - rightDiff;
        }
        return Math.abs(a.grossAmount - (interAmount ?? 0)) - Math.abs(b.grossAmount - (interAmount ?? 0));
      });
    }
    return data;
  }, [
    manualAcquirerRaw,
    manualRightSort.by,
    selectedInterdata,
    normalizedAppliedDateFrom,
    normalizedAppliedDateTo,
    appliedFilters.status,
    appliedFilters.brand,
    appliedFilters.paymentType,
    appliedFilters.search,
  ]);

  const manualAcquirerRows = useMemo<NormalizedUiRow[]>(
    () =>
      manualAcquirerFiltered.map((row: AcquirerUnifiedSale) => {
        const raw = (row.raw ?? {}) as Record<string, unknown>;
        const source = row.acquirer?.toLowerCase() || 'acquirer';
        const normalized = normalizeStandardRow(
          {
            ...raw,
            SALE_DATETIME: row.saleDatetime ?? raw.SALE_DATETIME,
            GROSS_AMOUNT: row.grossAmount ?? raw.GROSS_AMOUNT,
            NET_AMOUNT: row.netAmount ?? raw.NET_AMOUNT,
            MDR_AMOUNT: row.mdrAmount ?? raw.MDR_AMOUNT,
            ACQUIRER: row.acquirer ?? raw.ACQUIRER,
            STATUS: row.status ?? raw.STATUS,
            BRAND: row.brand ?? raw.BRAND,
            AUTH_NSU: row.nsu ?? raw.AUTH_NSU ?? raw.NSU,
            ACQ_NSU: row.nsu ?? raw.ACQ_NSU,
            SALE_NO:
              raw.SALE_NO ??
              raw.SALE_ID ??
              raw.ORDER_NO ??
              raw.SALE_RECEIPT,
          },
          source,
        );
        return {
          ...normalized,
          rowId: row.id,
          raw: row,
        };
      }),
    [manualAcquirerFiltered],
  );

  const total = normalizedRows.length;
  const canGoPrev = page > 1;
  const canGoNext = isDev && viewMode === 'reconciled' ? page * limit < rows.length : rows.length === limit;
  const selectedTitle =
    selected?.saleNoText ||
    selected?.nsuText ||
    selected?.operatorText ||
    'Registro';
  const pendingScopeLabel =
    pendingScope === 'INTERDATA'
      ? 'ERP (INTERDATA)'
      : `Adquirente (${pendingScope})`;
  const viewModeLabel =
    viewMode === 'pending' ? `Pendentes • ${pendingScopeLabel}` : 'Conciliados';

  const formatDisplayDate = (value: string) => {
    if (!value) {
      return '--';
    }
    const formatted = formatDateInput(value);
    return formatted || value;
  };

  const safeText = (value: unknown, fallback = '') => {
    const text = toTextAny(value);
    return text || fallback;
  };

  const dateSummary =
    appliedFilters.dateFrom || appliedFilters.dateTo
      ? `Data ${formatDisplayDate(appliedFilters.dateFrom)} - ${formatDisplayDate(appliedFilters.dateTo)}`
      : 'Sem filtro de data';
  const itemsSummary = `${total} itens`;
  const pageSummary = `Pag ${page}`;
  const grossSummary = totalsSummary.gross !== null ? formatMoneyBRL(totalsSummary.gross) : '--';
  const netSummary = totalsSummary.net !== null ? formatMoneyBRL(totalsSummary.net) : '--';
  const statusSummaryOrder = ['PENDENTE', 'DIVERGENTE', 'NAO_LOCALIZADO', 'AUTORIZADA'];
  const statusSummary = statusSummaryOrder
    .map((status) => ({
      label: status,
      total: totalsSummary.statusCounts[status],
    }))
    .filter((entry) => entry.total);

  const renderReconciliationDetails = (row: StandardRow) => {
    const rowId = row.rowId;
    if (!rowId) {
      return <div className="text-xs text-slate-500">Detalhes indisponiveis.</div>;
    }
    if (detailsLoadingById[rowId]) {
      return <div className="text-xs text-slate-500">Carregando...</div>;
    }
    const errorMessage = detailsErrorById[rowId];
    if (errorMessage) {
      return <div className="text-xs text-rose-600">{errorMessage}</div>;
    }
    const details = detailsById[rowId];
    if (!details) {
      return <div className="text-xs text-slate-500">Detalhes indisponiveis.</div>;
    }

    const formatFieldValue = (value: unknown) => {
      if (value === null || value === undefined || value === '') {
        return '--';
      }
      if (value instanceof Date) {
        return formatDatetimeCompact(value);
      }
      if (typeof value === 'object') {
        try {
          return JSON.stringify(value);
        } catch {
          return toTextAny(value);
        }
      }
      return toTextAny(value);
    };

    const formatAmount = (value: unknown) => {
      const parsed = typeof value === 'number' ? value : value ? Number(value) : NaN;
      return Number.isFinite(parsed) ? formatMoneyBRL(parsed) : '--';
    };
    const reconciliation = details.reconciliation;
    const interdataUi = details.interdataUi ?? {
      id: reconciliation?.INTERDATA_ID ?? null,
      saleNo: reconciliation?.SALE_NO ?? null,
      datetime: reconciliation?.SALE_DATETIME ?? null,
      grossAmount: reconciliation?.GROSS_AMOUNT ?? null,
      netAmount: reconciliation?.NET_AMOUNT ?? null,
      brand: reconciliation?.BRAND ?? null,
      status: reconciliation?.STATUS ?? null,
    };
    const acquirerUi = details.acquirerUi ?? {
      provider: reconciliation?.ACQUIRER ?? '',
      id: reconciliation?.ACQUIRER_ID ?? null,
      nsu: reconciliation?.ACQ_NSU ?? null,
      authCode: reconciliation?.ACQ_AUTH_CODE ?? null,
      datetime: reconciliation?.ACQ_SALE_DATETIME ?? null,
      grossAmount: reconciliation?.ACQ_GROSS_AMOUNT ?? null,
      netAmount: reconciliation?.ACQ_NET_AMOUNT ?? null,
      brand: reconciliation?.ACQ_BRAND ?? reconciliation?.BRAND ?? null,
      status: reconciliation?.STATUS ?? null,
    };
    const provider = acquirerUi?.provider || '';

    const showMissingNotice =
      details.missing?.match || details.missing?.interdata || details.missing?.acquirer;

    const interdataRaw = (details.interdataRaw ?? {}) as Record<string, unknown>;
    const acquirerRaw = (details.acquirerRaw ?? {}) as Record<string, unknown>;

    const readCanon = (
      source: Record<string, unknown>,
      candidates: string[],
      fallback?: unknown,
    ) => {
      for (const key of candidates) {
        const value = source[key];
        if (value !== undefined && value !== null && value !== '') {
          return formatFieldValue(value);
        }
      }
      return formatFieldValue(fallback);
    };
    const readRaw = (
      source: Record<string, unknown>,
      candidates: string[],
      fallback?: unknown,
    ) => {
      for (const key of candidates) {
        const value = source[key];
        if (value !== undefined && value !== null && value !== '') {
          return value;
        }
      }
      return fallback;
    };

    const formatPtBrDateTime = (value: unknown) => {
      if (value === null || value === undefined || value === '') {
        return '--';
      }
      const raw = toTextAny(value).trim();
      if (!raw) {
        return '--';
      }
      const dateOnlyMatch = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
      if (dateOnlyMatch) {
        return `${dateOnlyMatch[3]}/${dateOnlyMatch[2]}/${dateOnlyMatch[1]}`;
      }
      const parsed = new Date(raw);
      if (Number.isNaN(parsed.getTime())) {
        return raw;
      }
      return new Intl.DateTimeFormat('pt-BR', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
      }).format(parsed);
    };
    const normalizeMethodGroupLabel = (value: unknown) => {
      const raw = toTextAny(value).trim();
      if (!raw) {
        return '--';
      }
      const normalized = raw
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toUpperCase();
      if (normalized.includes('PIX')) {
        return 'PIX';
      }
      if (normalized.includes('CARD')) {
        return 'CARTÃO';
      }
      return normalized;
    };
    const normalizePaymentFlowLabel = (value: unknown, methodGroup?: string) => {
      const group = normalizeMethodGroupLabel(methodGroup);
      if (group === 'PIX') {
        return 'PIX';
      }
      const raw = toTextAny(value).trim();
      const normalized = raw
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toUpperCase();
      if (normalized.includes('PIX')) {
        return 'PIX';
      }
      if (normalized.includes('DEB') || normalized.includes('DEBIT')) {
        return 'DÉBITO';
      }
      if (normalized.includes('CRED') || normalized.includes('CREDIT')) {
        return 'CRÉDITO';
      }
      return '--';
    };
    const normalizeBrandLabel = (value: unknown) => {
      const raw = toTextAny(value).trim();
      if (!raw) {
        return '--';
      }
      const normalized = raw
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toUpperCase();
      if (normalized.includes('MAST')) {
        return 'MASTERCARD';
      }
      if (normalized.includes('VISA')) {
        return 'VISA';
      }
      if (normalized.includes('ELO')) {
        return 'ELO';
      }
      return normalized.split(/\s+/)[0] || '--';
    };

    const erpCanon = {
      saleDateRaw: readRaw(
        interdataRaw,
        ['SALE_DATETIME', 'TRANSACTION_DATETIME', 'DT_HR_VENDA', 'CREATED_AT'],
        readRaw(interdataRaw, ['CANON_SALE_DATE'], interdataUi?.datetime),
      ),
      methodGroup: readCanon(interdataRaw, ['CANON_METHOD_GROUP']),
      method: readCanon(interdataRaw, ['CANON_METHOD']),
      brand: readCanon(interdataRaw, ['CANON_BRAND'], interdataUi?.brand),
      terminal: readCanon(interdataRaw, ['CANON_TERMINAL_NO']),
      authCode: readCanon(interdataRaw, ['CANON_AUTH_CODE', 'AUTH_CODE']),
      nsu: readCanon(interdataRaw, ['CANON_NSU', 'NSU', 'AUTH_NSU']),
      gross: readCanon(interdataRaw, ['CANON_GROSS_AMOUNT'], interdataUi?.grossAmount),
      installmentNo: readCanon(interdataRaw, ['CANON_INSTALLMENT_NO']),
      installmentTotal: readCanon(interdataRaw, ['CANON_INSTALLMENT_TOTAL']),
    };

    const acqCanon = {
      saleDateRaw: readRaw(
        acquirerRaw,
        ['SALE_DATETIME', 'TRANSACTION_DATETIME', 'DT_HR_VENDA', 'CREATED_AT'],
        readRaw(acquirerRaw, ['CANON_SALE_DATE'], acquirerUi?.datetime),
      ),
      methodGroup: readCanon(acquirerRaw, ['CANON_METHOD_GROUP']),
      method: readCanon(acquirerRaw, ['CANON_METHOD']),
      brand: readCanon(acquirerRaw, ['CANON_BRAND'], acquirerUi?.brand),
      terminal: readCanon(acquirerRaw, ['CANON_TERMINAL_NO']),
      authCode: readCanon(acquirerRaw, ['CANON_AUTH_CODE', 'AUTH_CODE', 'AUTH_NO'], acquirerUi?.authCode),
      nsu: readCanon(acquirerRaw, ['CANON_NSU', 'NSU', 'NSU_DOC', 'TRANSACTION_NO'], acquirerUi?.nsu),
      gross: readCanon(acquirerRaw, ['CANON_GROSS_AMOUNT'], acquirerUi?.grossAmount),
      installmentNo: readCanon(acquirerRaw, ['CANON_INSTALLMENT_NO']),
      installmentTotal: readCanon(acquirerRaw, ['CANON_INSTALLMENT_TOTAL']),
    };
    const erpDisplay = {
      saleDate: formatPtBrDateTime(erpCanon.saleDateRaw),
      methodGroup: normalizeMethodGroupLabel(erpCanon.methodGroup),
      method: normalizePaymentFlowLabel(erpCanon.method, erpCanon.methodGroup),
      brand: normalizeBrandLabel(erpCanon.brand),
    };
    const acqDisplay = {
      saleDate: formatPtBrDateTime(acqCanon.saleDateRaw),
      methodGroup: normalizeMethodGroupLabel(acqCanon.methodGroup),
      method: normalizePaymentFlowLabel(acqCanon.method, acqCanon.methodGroup),
      brand: normalizeBrandLabel(acqCanon.brand),
    };

    return (
      <div className="flex flex-col gap-3">
        {showMissingNotice ? (
          <div className="rounded-xl border border-amber-200 bg-amber-50 p-3 text-xs text-amber-700">
            Não ha trilha do match para este conciliado (conciliado antigo ou IDs ausentes). Reconciliar novamente para gerar trilha.
          </div>
        ) : null}
        <div className="overflow-x-hidden rounded-xl border border-slate-200 bg-white">
          <table className="w-full table-fixed border-collapse text-[11px] text-slate-700">
            <thead className="bg-slate-50 text-slate-600">
              <tr>
                <th className="w-[14%] border-b border-slate-200 px-2 py-0 text-left font-semibold">Origem</th>
                <th className="w-[16%] border-b border-slate-200 px-2 py-0 text-left font-semibold">Data/Hora</th>
                <th className="w-[14%] border-b border-slate-200 px-2 py-0 text-left font-semibold">Mtd Pag</th>
                <th className="w-[14%] border-b border-slate-200 px-2 py-0 text-left font-semibold">Deb/Cred/Pix</th>
                <th className="w-[14%] border-b border-slate-200 px-2 py-0 text-left font-semibold">Bandeira</th>
                {/*<th className="w-[10%] border-b border-slate-200 px-2 py-2 text-left font-semibold">NSU</th>*/}
                {/*<th className="w-[10%] border-b border-slate-200 px-2 py-2 text-left font-semibold">Codigo de Autorização</th>*/}
                <th className="w-[14%] border-b border-slate-200 px-2 py-0 text-left font-semibold">NSU</th>
                <th className="w-[14%] border-b border-slate-200 px-2 py-0 text-left font-semibold">Valor</th>
                {/*<th className="w-[7%] border-b border-slate-200 px-2 py-2 text-left font-semibold">Estabelecimento</th>
                <th className="w-[8%] border-b border-slate-200 px-2 py-2 text-left font-semibold">Est TOTAL</th>*/}
              </tr>
            </thead>
            <tbody>
              <tr className="bg-white">
                <td className="border-b border-slate-100 px-2 py-0 font-semibold text-slate-700">ERP(INTERDATA)</td>
                <td className="border-b border-slate-100 px-2 py-0">{erpDisplay.saleDate}</td>
                <td className="border-b border-slate-100 px-2 py-0">{erpDisplay.methodGroup}</td>
                <td className="border-b border-slate-100 px-2 py-0">{erpDisplay.method}</td>
                <td className="border-b border-slate-100 px-2 py-0">{erpDisplay.brand}</td>
                {/*<td className="border-b border-slate-100 px-2 py-2">{erpCanon.terminal}</td>
                <td className="border-b border-slate-100 px-2 py-2">{erpCanon.authCode}</td>*/}
                <td className="border-b border-slate-100 px-2 py-0">{erpCanon.nsu}</td>
                <td className="border-b border-slate-100 px-2 py-0">{erpCanon.gross}</td>
                {/*<td className="border-b border-slate-100 px-2 py-2">{erpCanon.installmentNo}</td>
                <td className="border-b border-slate-100 px-2 py-2">{erpCanon.installmentTotal}</td>*/}
              </tr>
              <tr className="bg-slate-50/40">
                <td className="border-b border-slate-100 px-2 py-0 font-semibold text-slate-700">
                  ADQUIRENTE ({provider || '--'})
                </td>
                <td className="border-b border-slate-100 px-2 py-0">{acqDisplay.saleDate}</td>
                <td className="border-b border-slate-100 px-2 py-0">{acqDisplay.methodGroup}</td>
                <td className="border-b border-slate-100 px-2 py-0">{acqDisplay.method}</td>
                <td className="border-b border-slate-100 px-2 py-0">{acqDisplay.brand}</td>
                {/*<td className="border-b border-slate-100 px-2 py-2">{acqCanon.terminal}</td>
                <td className="border-b border-slate-100 px-2 py-2">{acqCanon.authCode}</td>*/}
                <td className="border-b border-slate-100 px-2 py-0">{acqCanon.nsu}</td>
                <td className="border-b border-slate-100 px-2 py-0">{acqCanon.gross}</td>
                {/*<td className="border-b border-slate-100 px-2 py-2">{acqCanon.installmentNo}</td>
                <td className="border-b border-slate-100 px-2 py-2">{acqCanon.installmentTotal}</td>*/}
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  const handleViewModeChange = (nextMode: 'pending' | 'reconciled') => {
    if (viewMode === nextMode) {
      return;
    }
    setViewMode(nextMode);
    setPage(1);
    setSelected(null);
    setSelectedId(null);
    setExpandedId(null);
    if (manualMode) {
      setManualMode(false);
    }
    if (nextMode === 'reconciled') {
      const nextFilters = {
        dateFrom: normalizeDate(appliedFilters.dateFrom),
        dateTo: normalizeDate(appliedFilters.dateTo),
        status: '',
        search: '',
        paymentType: '',
        brand: '',
      };
      setSearchParams(buildSearchParams(nextFilters));
    }
  };

  const handleManualConfirm = async () => {
    if (!selectedInterdata?.ID || !selectedAcquirer?.id || !manualReason) {
      return;
    }
    if (selectedAcquirer.acquirer === 'SICREDI') {
      window.alert('Conciliação manual para Sicredi ainda não suportada.');
      return;
    }

    const interAmount =
      typeof selectedInterdata.GROSS_AMOUNT === 'number' ? selectedInterdata.GROSS_AMOUNT : null;
    const acqAmount =
      typeof selectedAcquirer.grossAmount === 'number' ? selectedAcquirer.grossAmount : null;
    if (
      interAmount !== null &&
      acqAmount !== null &&
      Math.abs(interAmount - acqAmount) > 0.01
    ) {
      const proceed = window.confirm('Valor diferente. Deseja conciliar mesmo assim?');
      if (!proceed) {
        return;
      }
    }

    const interDate = parseDateSafe(selectedInterdata.SALE_DATETIME ?? null);
    const acqDate = parseDateSafe(selectedAcquirer.saleDatetime ?? null);
    if (interDate && acqDate) {
      const diffDays = Math.abs(interDate.getTime() - acqDate.getTime()) / (1000 * 60 * 60 * 24);
      if (diffDays > 1) {
        const proceed = window.confirm('Data distante. Deseja conciliar mesmo assim?');
        if (!proceed) {
          return;
        }
      }
    }

    try {
      await manualMutation.mutateAsync({
        interdataId: selectedInterdata.ID,
        acquirerProvider: selectedAcquirer.acquirer as 'CIELO' | 'SIPAG',
        acquirerSaleId: selectedAcquirer.id,
        reason: manualReason as 'TIME_DIFF' | 'NSU_DIFF' | 'BATCH' | 'OTHER',
        notes: manualNotes.trim() ? manualNotes.trim() : undefined,
      });
      window.alert('Conciliação manual gravada ✅');
      setSelectedInterdata(null);
      setSelectedAcquirer(null);
      setManualReason('');
      setManualNotes('');
    } catch (error: any) {
      const message = error?.response?.data?.error || error?.message || 'Erro ao conciliar manualmente';
      window.alert(message);
    }
  };

  return (
    <div className="flex h-full min-h-0 flex-col gap-2 overflow-hidden">
      {/*<header>
        <h1 className="text-2xl font-semibold text-slate-900">Conciliacao</h1>
        <p className="text-sm text-slate-500">Modelo 1 - foco em grandes volumes e filtros rapidos.</p>
      </header>*/}

      <ReconciliationFilters
        draftFilters={draftFilters}
        onDraftChange={(values) => {
          setDraftFilters(values);
          if (dateError) {
            setDateError('');
          }
        }}
        isFetching={query.isFetching}
        dateError={dateError}
        onApply={() => {
          setPage(1);
          setSelected(null);
          const normalizedDateFrom = normalizeDate(draftFilters.dateFrom);
          const normalizedDateTo = normalizeDate(draftFilters.dateTo);
          if (
            (draftFilters.dateFrom && !normalizedDateFrom) ||
            (draftFilters.dateTo && !normalizedDateTo)
          ) {
            setDateError('Data invalida. Use o calendario dos campos de data.');
            return;
          }
          const normalizedDraft = {
            ...draftFilters,
            dateFrom: normalizedDateFrom,
            dateTo: normalizedDateTo,
          };
          setSearchParams(buildSearchParams(normalizedDraft));
          dlog('Aplicando filtros', normalizedDraft);
        }}
        onReset={() => {
          setPage(1);
          setDraftFilters(defaultFilters);
          setDateError('');
          setSelected(null);
          setSearchParams({});
        }}
        showAdvanced
        statusOptions={filterOptions.statusOptions}
        paymentOptions={filterOptions.paymentOptions}
        brandOptions={brandOptionsForDraftPayment}
        brandDisabled={isDraftPaymentPix}
      />

      {query.isError ? (
        <Card className="border border-rose-200 bg-rose-50 p-4">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div>
              <p className="text-sm font-semibold text-rose-700">Falha ao carregar dados</p>
              <p className="text-xs text-rose-600">
                {(query.error as Error)?.message || 'Erro inesperado na API.'}
              </p>
            </div>
            <Button onClick={() => query.refetch()}>Tentar novamente</Button>
          </div>
        </Card>
      ) : null}

      {manualMode ? (
        <div className={styles.manualMode}>
          <Card className={styles.manualPanel}>
            <div className={styles.manualHeader}>
              <div className={styles.manualHeaderLeft}>
                <span className="text-xs font-semibold uppercase text-slate-500">
                  Conciliação manual
                </span>
                <span className={styles.manualMeta}>
                  Pendente:{' '}
                  {selectedInterdata
                    ? `${formatDatetimeCompact(selectedInterdata.SALE_DATETIME)} • ${
                        selectedInterdata.SALE_NO ||
                        selectedInterdata.AUTH_NSU ||
                        'Sem venda'
                      } • ${formatMoneyBRL(selectedInterdata.GROSS_AMOUNT ?? null)}`
                    : 'nenhum'}
                </span>
                <span className={styles.manualMeta}>
                  Adquirente:{' '}
                  {selectedAcquirer
                    ? `${selectedAcquirer.acquirer} • ${formatDatetimeCompact(
                        selectedAcquirer.saleDatetime,
                      )} • ${formatMoneyBRL(selectedAcquirer.grossAmount ?? null)}`
                    : 'nenhum'}
                </span>
              </div>
              <Button
                variant="ghost"
                className="h-8 px-2 py-0 text-[11px]"
                onClick={() => setManualMode(false)}
              >
                Cancelar
              </Button>
            </div>
            <div className={styles.manualActionsRow}>
              <div className={styles.manualMotivo}>
                <label>Motivo</label>
                <Select
                  className="h-8 px-2 text-[11px]"
                  value={manualReason}
                  onChange={(event) =>
                    setManualReason(event.target.value as 'TIME_DIFF' | 'NSU_DIFF' | 'BATCH' | 'OTHER' | '')
                  }
                >
                  <option value="">Selecione</option>
                  <option value="TIME_DIFF">Diferença de horário</option>
                  <option value="NSU_DIFF">NSU divergente</option>
                  <option value="BATCH">Venda lançada em lote</option>
                  <option value="OTHER">Outro</option>
                </Select>
              </div>
              <div className={styles.manualObs}>
                <label>Observação</label>
                <Input
                  className="h-8 px-2 text-[11px]"
                  placeholder="Opcional"
                  value={manualNotes}
                  onChange={(event) => setManualNotes(event.target.value)}
                />
              </div>
              <div className={styles.manualButtons}>
                <Button
                  className="h-8 px-2 py-0 text-[11px]"
                  disabled={
                    !selectedInterdata ||
                    !selectedAcquirer ||
                    !manualReason ||
                    selectedAcquirer.acquirer === 'SICREDI' ||
                    manualMutation.isPending
                  }
                  onClick={handleManualConfirm}
                >
                  Confirmar
                </Button>
              </div>
            </div>
          </Card>

          <div className={styles.manualTablesGrid}>
            <div className={`${styles.tableCard} rounded-2xl border border-slate-100 bg-white shadow-card`}>
              <div className="flex items-center justify-between gap-2 border-b border-slate-100 px-3 py-2 text-xs text-slate-600">
                <span className="font-semibold uppercase text-slate-500">Pendentes</span>
                <div className="flex items-center gap-2">
                  <Button
                    variant="outline"
                    className="h-8 px-2 py-0 text-[11px]"
                    disabled={manualPendingPage <= 1 || pendingQuery.isFetching}
                    onClick={() => setManualPendingPage((current) => Math.max(1, current - 1))}
                  >
                    Anterior
                  </Button>
                  <Button
                    variant="outline"
                    className="h-8 px-2 py-0 text-[11px]"
                    disabled={manualPendingRaw.length < limit || pendingQuery.isFetching}
                    onClick={() => setManualPendingPage((current) => current + 1)}
                  >
                    Proxima
                  </Button>
                </div>
              </div>
              <div className={`${styles.tableBody} flex-1 min-h-0 overflow-y-auto overflow-x-auto scrollbar-thin ${styles.tableShell}`}>
                <StandardTable
                  rows={manualPendingRows}
                  loading={pendingQuery.isFetching}
                  hideFeesAndNet
                  selectedRowId={selectedInterdata?.ID ?? null}
                  onRowClick={(row) => setSelectedInterdata(row.raw as InterdataSale)}
                  enableHorizontalScroll
                  scrollWrapperClassName="w-full overflow-x-auto"
                  sortBy={manualLeftSort.by}
                  sortDir={manualLeftSort.dir}
                  onSortChange={(next) => {
                    setManualPendingPage(1);
                    setManualLeftSort(next);
                  }}
                />
              </div>
            </div>

            <div className={`${styles.tableCard} rounded-2xl border border-slate-100 bg-white shadow-card`}>
              <div className="flex flex-wrap items-center gap-2 border-b border-slate-100 px-3 py-2 text-xs text-slate-600">
                <div className="flex items-center gap-2">
                  <Button
                    variant={manualAcquirerFilter === 'ALL' ? undefined : 'outline'}
                    className="h-8 px-2 py-0 text-[11px]"
                    onClick={() => setManualAcquirerFilter('ALL')}
                  >
                    Todas
                  </Button>
                  {/*<Button
                    variant={manualAcquirerFilter === 'CIELO' ? undefined : 'outline'}
                    className="h-8 px-2 py-0 text-[11px]"
                    onClick={() => setManualAcquirerFilter('CIELO')}
                  >
                    Cielo
                  </Button>
                  <Button
                    variant={manualAcquirerFilter === 'SIPAG' ? undefined : 'outline'}
                    className="h-8 px-2 py-0 text-[11px]"
                    onClick={() => setManualAcquirerFilter('SIPAG')}
                  >
                    Sipag
                  </Button>
                  <Button
                    variant={manualAcquirerFilter === 'SICREDI' ? undefined : 'outline'}
                    className="h-8 px-2 py-0 text-[11px]"
                    onClick={() => setManualAcquirerFilter('SICREDI')}
                  >
                    Sicredi
                  </Button>*/}
                </div>
                {/*<div className="flex flex-1 items-center gap-2">
                  <Input
                    className="h-8 px-2 text-[11px]"
                    placeholder="Busca rapida"
                    value={manualSearch}
                    onChange={(event) => setManualSearch(event.target.value)}
                  />
                </div>*/}
                <div className="flex items-center gap-2">
                  <Button
                    variant="outline"
                    className="h-8 px-2 py-0 text-[11px]"
                    disabled={manualAcquirerPage <= 1 || acquirerQuery.isFetching}
                    onClick={() => setManualAcquirerPage((current) => Math.max(1, current - 1))}
                  >
                    Anterior
                  </Button>
                  <Button
                    variant="outline"
                    className="h-8 px-2 py-0 text-[11px]"
                    disabled={manualAcquirerRaw.length < limit || acquirerQuery.isFetching}
                    onClick={() => setManualAcquirerPage((current) => current + 1)}
                  >
                    Proxima
                  </Button>
                </div>
              </div>
              <div className={`${styles.tableBody} flex-1 min-h-0 overflow-y-auto overflow-x-auto scrollbar-thin ${styles.tableShell}`}>
                <StandardTable
                  rows={manualAcquirerRows}
                  loading={acquirerQuery.isFetching}
                  selectedRowId={selectedAcquirer?.id ?? null}
                  onRowClick={(row) => setSelectedAcquirer(row.raw as AcquirerUnifiedSale)}
                  enableHorizontalScroll
                  scrollWrapperClassName="w-full overflow-x-auto"
                  sortBy={manualRightSort.by}
                  sortDir={manualRightSort.dir}
                  onSortChange={(next) => {
                    setManualAcquirerPage(1);
                    setManualRightSort(next);
                  }}
                />
              </div>
            </div>
          </div>
        </div>
      ) : (
        <>
          <div className={styles.summaryHeader}>
            <div className={styles.summaryLeft}>
              <div className={styles.summaryTitle}>
                <span className={styles.summaryLabel}>Resumo</span>
                <span className={styles.summaryMode}>{viewModeLabel}</span>
                {previewActive && viewMode === 'reconciled' ? (
                  <span className={styles.previewBadge}>PREVIEW (dryRun)</span>
                ) : null}
              </div>
              <div className={styles.summaryLine}>
                <span>{dateSummary}</span>
                <span>|</span>
                <span>{itemsSummary}</span>
                <span>|</span>
                <span>{pageSummary}</span>
                <span>|</span>
                <span>Bruto {grossSummary}</span>
                <span>|</span>
                <span>Liquido {netSummary}</span>
              </div>
              {viewMode !== 'reconciled' && statusSummary.length ? (
                <div className={styles.summaryStatuses}>
                  {statusSummary.map((entry) => (
                    <span key={entry.label}>
                      {entry.label} {entry.total}
                    </span>
                  ))}
                </div>
              ) : null}
            </div>
            <div className={styles.summaryRight}>
              <div className={styles.summaryModes}>
                <Button
                  variant={viewMode === 'pending' ? undefined : 'outline'}
                  className="h-8 px-2 py-0 text-[11px]"
                  onClick={() => handleViewModeChange('pending')}
                >
                  Pendentes
                </Button>
                <Button
                  variant={viewMode === 'reconciled' ? undefined : 'outline'}
                  className="h-8 px-2 py-0 text-[11px]"
                  onClick={() => handleViewModeChange('reconciled')}
                >
                  Conciliados
                </Button>
              </div>
            </div>
            {viewMode === 'pending' ? (
              <div className={styles.pendingControlsRow}>
                <div className="flex items-center gap-2">
                  <div className={styles.pendingManualGroup}>
                    <Button
                      variant="outline"
                      className="flex h-8 items-center gap-1.5 rounded-l-full rounded-r-md px-2 py-0 text-[11px]"
                      title="Abrir tela de conciliação manual"
                      onClick={() => {
                        setViewMode('pending');
                        setManualMode(true);
                      }}
                    >
                      <svg
                        className="h-3.5 w-3.5"
                        viewBox="0 0 24 24"
                        fill="none"
                        xmlns="http://www.w3.org/2000/svg"
                        aria-hidden="true"
                      >
                        <path
                          d="M7 11V6a1.5 1.5 0 1 1 3 0v4"
                          stroke="currentColor"
                          strokeWidth="2"
                          strokeLinecap="round"
                          strokeLinejoin="round"
                        />
                        <path
                          d="M10 10V5.5a1.5 1.5 0 1 1 3 0V10"
                          stroke="currentColor"
                          strokeWidth="2"
                          strokeLinecap="round"
                          strokeLinejoin="round"
                        />
                        <path
                          d="M13 10V6.5a1.5 1.5 0 1 1 3 0V12"
                          stroke="currentColor"
                          strokeWidth="2"
                          strokeLinecap="round"
                          strokeLinejoin="round"
                        />
                        <path
                          d="M16 11.5a1.5 1.5 0 1 1 3 0v1.5c0 3.314-2.686 6-6 6h-1a5 5 0 0 1-5-5v-2.3a1.7 1.7 0 0 1 3.4 0V13"
                          stroke="currentColor"
                          strokeWidth="2"
                          strokeLinecap="round"
                          strokeLinejoin="round"
                        />
                      </svg>
                      Conciliação manual
                    </Button>
                    <Button
                      variant="outline"
                      className="flex h-8 items-center gap-1.5 rounded-l-md rounded-r-full px-2 py-0 text-[11px]"
                      title={autoReconciliationTitle}
                      disabled={reconciliationTrigger.isPending || autoReconWaiting}
                      onClick={() => {
                        if (reconciliationTrigger.isPending || autoReconWaiting) {
                          return;
                        }
                        setAutoReconWaiting(true);
                        reconciliationTrigger.mutate({
                          dateFrom: normalizedAppliedDateFrom || undefined,
                          dateTo: todayDate,
                        });
                      }}
                    >
                      {reconciliationTrigger.isPending || autoReconWaiting ? (
                        <svg
                          className="h-3.5 w-3.5 animate-spin"
                          viewBox="0 0 24 24"
                          fill="none"
                          xmlns="http://www.w3.org/2000/svg"
                          aria-hidden="true"
                        >
                          <circle
                            cx="12"
                            cy="12"
                            r="9"
                            className="opacity-25"
                            stroke="currentColor"
                            strokeWidth="3"
                          />
                          <path
                            d="M21 12a9 9 0 0 0-9-9"
                            className="opacity-90"
                            stroke="currentColor"
                            strokeWidth="3"
                            strokeLinecap="round"
                          />
                        </svg>
                      ) : (
                        <svg
                          className="h-3.5 w-3.5"
                          viewBox="0 0 24 24"
                          fill="none"
                          xmlns="http://www.w3.org/2000/svg"
                          aria-hidden="true"
                        >
                          <path
                            d="M20 11a8 8 0 1 0 2.34 5.66"
                            stroke="currentColor"
                            strokeWidth="2"
                            strokeLinecap="round"
                            strokeLinejoin="round"
                          />
                          <path
                            d="M20 4v7h-7"
                            stroke="currentColor"
                            strokeWidth="2"
                            strokeLinecap="round"
                            strokeLinejoin="round"
                          />
                        </svg>
                      )}
                      {autoReconWaiting ? 'Aguardando conciliação...' : autoReconciliationLabel}
                    </Button>
                  </div>
                </div>
                <div className={styles.pendingScopeGroup}>
                  <Button
                    variant={pendingScope === 'INTERDATA' ? undefined : 'outline'}
                    className="h-8 rounded-l-full rounded-r-md px-2 py-0 text-[11px]"
                    onClick={() => setPendingScope('INTERDATA')}
                  >
                    ERP (INTERDATA)
                  </Button>
                  <Button
                    variant={pendingScope === 'CIELO' ? undefined : 'outline'}
                    className="h-8 rounded-md px-2 py-0 text-[11px]"
                    onClick={() => setPendingScope('CIELO')}
                  >
                    CIELO
                  </Button>
                  <Button
                    variant={pendingScope === 'SIPAG' ? undefined : 'outline'}
                    className="h-8 rounded-md px-2 py-0 text-[11px]"
                    onClick={() => setPendingScope('SIPAG')}
                  >
                    SIPAG
                  </Button>
                  <Button
                    variant={pendingScope === 'SICREDI' ? undefined : 'outline'}
                    className="h-8 rounded-l-md rounded-r-full px-2 py-0 text-[11px]"
                    onClick={() => setPendingScope('SICREDI')}
                  >
                    SICREDI
                  </Button>
                </div>
              </div>
            ) : null}
            {/*<Button
          variant="outline"
          onClick={() => {
            const headers = [
              'DataHora',
              'Origem',
              'Operadora',
              'NSU',
              'Bandeira',
              'Tipo',
              'Parcelas',
              'Valor',
              'Status',
            ];
            const lines = rows.map((sale) => [
              toText(sale.SALE_DATETIME ?? ''),
              toText(sale.SOURCE ?? 'INTERDATA') || 'INTERDATA',
              toText(sale.SOURCE ?? 'INTERDATA') || 'INTERDATA',
              toText(sale.AUTH_NSU ?? ''),
              toText(sale.CARD_BRAND_RAW ?? ''),
              toText(sale.CARD_MODE ?? sale.PAYMENT_TYPE ?? ''),
              toText(sale.INSTALLMENTS ?? ''),
              toText(sale.GROSS_AMOUNT ?? ''),
              deriveStatus(sale),
            ]);
            const toCsv = [headers, ...lines]
              .map((line) =>
                line
                  .map((item) => `"${String(item ?? '').replace(/"/g, '""')}"`)
                  .join(','),
              )
              .join('\n');
            const blob = new Blob([toCsv], { type: 'text/csv;charset=utf-8;' });
            const url = URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = `conciliacao-${Date.now()}.csv`;
            document.body.appendChild(link);
            link.click();
            link.remove();
            URL.revokeObjectURL(url);
          }}
        >
          Exportar CSV
        </Button>*/}
          </div>

          <div className="flex flex-1 min-h-0 flex-col overflow-hidden">
            <div className="flex flex-1 min-h-0 items-stretch gap-2">
              <Button
                variant="outline"
                className="my-auto h-9 w-9 rounded-full p-0 text-base"
                disabled={!canGoPrev || query.isFetching}
                onClick={() => setPage((current) => Math.max(1, current - 1))}
                aria-label="Página anterior"
                title="Página anterior"
              >
                ‹
              </Button>
              <div className={`flex-1 min-h-0 overflow-auto scrollbar-thin ${styles.tableShell}`}>
                <StandardTable
                  rows={normalizedRows}
                  loading={query.isLoading}
                  hideFeesAndNet={viewMode === 'pending' && pendingScope === 'INTERDATA'}
                  onRowClick={(row) => {
                    if (viewMode === 'reconciled' && !previewActive) {
                      setSelected(null);
                      setSelectedId(row.rowId ?? null);
                      handleReconciledRowClick(row);
                    } else {
                      setSelected(row);
                      setSelectedId(null);
                    }
                  }}
                  showOrigin={viewMode === 'reconciled'}
                  showMatch={viewMode === 'reconciled'}
                  selectedRowId={viewMode === 'reconciled' && !previewActive ? selectedId : null}
                  expandedRowId={viewMode === 'reconciled' && !previewActive ? expandedId : null}
                  renderExpandedRow={
                    viewMode === 'reconciled' && !previewActive ? renderReconciliationDetails : undefined
                  }
                  rowClassName={viewMode === 'reconciled' ? styles.row : undefined}
                  selectedRowClassName={
                    viewMode === 'reconciled' && !previewActive ? styles.rowSelected : undefined
                  }
                  expandedRowClassName={
                    viewMode === 'reconciled' && !previewActive ? styles.rowExpanded : undefined
                  }
                  sortBy={currentSort.by}
                  sortDir={currentSort.dir}
                  onSortChange={(next) => {
                    setPage(1);
                    if (viewMode === 'pending') {
                      setPendingSort(next);
                    } else {
                      setReconciledSort(next);
                    }
                  }}
                />
              </div>
              <Button
                variant="outline"
                className="my-auto h-9 w-9 rounded-full p-0 text-base"
                disabled={!canGoNext || query.isFetching}
                onClick={() => setPage((current) => current + 1)}
                aria-label="Próxima página"
                title="Próxima página"
              >
                ›
              </Button>
            </div>
          </div>
        </>
      )}

      {selected ? (
        <div className="fixed right-3 top-20 z-40 h-[70vh] w-full max-w-[calc(100vw-1.5rem)] overflow-auto rounded-2xl border border-slate-200 bg-white shadow-card sm:right-6 sm:top-28 sm:w-[380px] sm:max-w-[380px]">
          <div className="flex items-center justify-between border-b border-slate-100 px-4 py-3">
            <div>
              <p className="text-xs uppercase text-slate-400">Detalhes</p>
              <p className="text-sm font-semibold text-slate-800">{selectedTitle}</p>
            </div>
            <Button variant="ghost" onClick={() => setSelected(null)}>
              Fechar
            </Button>
          </div>
          <div className="p-4">
            {selected.originText?.toUpperCase() === 'MANUAL' ? (
              <div className="mb-3 rounded-xl border border-slate-200 bg-slate-50 p-3 text-xs text-slate-600">
                <p className="font-semibold uppercase text-slate-500">Conciliação manual</p>
                <p>Motivo: {selected.reasonText || 'Nao informado'}</p>
                <p>Observacao: {selected.notesText || 'Sem observacao'}</p>
              </div>
            ) : null}
            <pre className="whitespace-pre-wrap text-xs text-slate-600">
              {JSON.stringify(selected, null, 2)}
            </pre>
          </div>
        </div>
      ) : null}
    </div>
  );
};
