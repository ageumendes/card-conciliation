import * as xlsx from 'xlsx';
import { NormalizedSale, ParseResult } from './interdata-import.types';
import { hashRow, isValidDate, parseInstallments, parseMoney } from './interdata-import.utils';

const normalizeText = (value: unknown): string => {
  if (value === null || value === undefined) {
    return '';
  }
  return String(value).trim();
};

const normalizeUpper = (value: unknown): string => normalizeText(value).toUpperCase();

const normalizeHeader = (value: unknown): string => {
  const text = normalizeText(value).toLowerCase();
  const noAccent = text.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  return noAccent.replace(/[^a-z0-9]+/g, ' ').trim();
};

const isHeaderLikeRow = (values: Array<unknown>): boolean => {
  const normalized = values.map((value) => normalizeHeader(value));
  const hasVenda = normalized.some((value) => value.includes('venda'));
  const hasStatus = normalized.some((value) => value.includes('status'));
  const hasBandeira = normalized.some((value) => value.includes('bandeira'));
  const hasOperacao = normalized.some((value) => value.includes('op'));

  if (hasVenda && (hasStatus || hasBandeira || hasOperacao)) {
    return true;
  }

  return normalized.some((value) =>
    [
      'n venda',
      'numero venda',
      'bandeira',
      'status',
      'status consiliacao',
      'n op',
      'no op',
      'data venda',
      'data e hora da venda',
    ].includes(value),
  );
};

const headerAliases = {
  saleNo: ['numero venda', 'n venda', 'cupom', 'documento', 'doc', 'no venda', 'n0 venda'],
  date: ['data', 'dt', 'data venda', 'emissao', 'data e hora da venda', 'data hora da venda', 'data hora', 'data/hora'],
  time: ['hora', 'hr'],
  total: ['valor', 'total', 'valor total', 'vlr', 'vl total'],
  payment: [
    'pagamento',
    'forma',
    'forma pagto',
    'tipo',
    'especie pag',
    'especie pagamento',
    'espeie pag',
  ],
  nsu: ['nsu', 'n op', 'no op', 'numero op', 'n operacao', 'no operacao'],
  auth: ['autorizacao', 'cod autorizacao', 'auth', 'authorization'],
  brand: ['bandeira', 'flag'],
  acquirer: ['adquirente', 'operadora'],
  tid: ['tid'],
  status: ['status', 'situacao', 'situacao venda'],
  installments: ['parcela', 'parcelas', 'n parcelas'],
  fees: ['taxa', 'tarifa'],
  net: ['liquido', 'net'],
};

const findHeaderRow = (rows: unknown[][]) => {
  let best: { index: number; mapping: Record<string, number | null>; score: number } | null = null;

  for (let i = 0; i < Math.min(rows.length, 5); i += 1) {
    const row = rows[i];
    const mapping: Record<string, number | null> = {
      saleNoIndex: null,
      dateIndex: null,
      dateFallbackIndex: null,
      timeIndex: null,
      totalIndex: null,
      paymentIndex: null,
      nsuIndex: null,
      authIndex: null,
      brandIndex: null,
      acquirerIndex: null,
      tidIndex: null,
      statusIndex: null,
      installmentsIndex: null,
      feesIndex: null,
      netIndex: null,
    };

    row.forEach((cell, idx) => {
      const header = normalizeHeader(cell);
      const match = (aliases: string[]) => aliases.some((alias) => header === alias || header.includes(alias));

      if (header.includes('data e hora') || header.includes('data/hora') || header.includes('data hora')) {
        mapping.dateIndex = idx;
      } else if (header.includes('data venda') && mapping.dateFallbackIndex === null) {
        mapping.dateFallbackIndex = idx;
      } else if (match(headerAliases.date)) {
        if (mapping.dateIndex === null) {
          mapping.dateIndex = idx;
        } else if (mapping.dateFallbackIndex === null) {
          mapping.dateFallbackIndex = idx;
        }
      } else if (match(headerAliases.time)) {
        mapping.timeIndex = idx;
      } else if (match(headerAliases.saleNo)) {
        mapping.saleNoIndex = idx;
      } else if (match(headerAliases.total)) {
        mapping.totalIndex = idx;
      } else if (match(headerAliases.payment)) {
        mapping.paymentIndex = idx;
      } else if (match(headerAliases.nsu)) {
        mapping.nsuIndex = idx;
      } else if (match(headerAliases.auth)) {
        mapping.authIndex = idx;
      } else if (match(headerAliases.brand)) {
        mapping.brandIndex = idx;
      } else if (match(headerAliases.acquirer)) {
        mapping.acquirerIndex = idx;
      } else if (match(headerAliases.tid)) {
        mapping.tidIndex = idx;
      } else if (match(headerAliases.status)) {
        mapping.statusIndex = idx;
      } else if (match(headerAliases.installments)) {
        mapping.installmentsIndex = idx;
      } else if (match(headerAliases.fees)) {
        mapping.feesIndex = idx;
      } else if (match(headerAliases.net)) {
        mapping.netIndex = idx;
      }
    });

    const score = [mapping.saleNoIndex, mapping.totalIndex, mapping.dateIndex].filter(
      (value) => value !== null,
    ).length;

    if (mapping.saleNoIndex !== null && mapping.totalIndex !== null && mapping.dateIndex !== null) {
      if (!best || score > best.score) {
        best = { index: i, mapping, score };
      }
    }
  }

  return best;
};

const detectColumnsLegacy = (rows: unknown[][]) => {
  const scores = {
    value: new Map<number, number>(),
    brand: new Map<number, number>(),
    status: new Map<number, number>(),
    installments: new Map<number, number>(),
    type: new Map<number, number>(),
    nsu: new Map<number, number>(),
    fees: new Map<number, number>(),
    net: new Map<number, number>(),
  };

  const updateScore = (map: Map<number, number>, index: number) => {
    map.set(index, (map.get(index) ?? 0) + 1);
  };

  const maxCols = Math.max(...rows.map((row) => row.length), 0);

  rows.forEach((row) => {
    for (let i = 0; i < maxCols; i += 1) {
      const value = row[i];
      const text = normalizeUpper(value);

      if (text.includes('VISA') || text.includes('MASTER') || text.includes('ELO') || text.includes('PIX')) {
        updateScore(scores.brand, i);
      }

      if (text.includes('ATIVO') || text.includes('CANCEL') || text.includes('ESTORN')) {
        updateScore(scores.status, i);
      }

      if (/(\d+)\s*\/\s*(\d+)/.test(text)) {
        updateScore(scores.installments, i);
      }

      if (text.includes('CREDITO') || text.includes('CRÉDITO') || text.includes('DEBITO') || text.includes('DÉBITO')) {
        updateScore(scores.type, i);
      }

      if (/^\d{6,}$/.test(text)) {
        updateScore(scores.nsu, i);
      }

      const money = parseMoney(value);
      if (money !== null && money > 0) {
        updateScore(scores.value, i);
      }

      if (text.includes('TAXA') || text.includes('TARIFA')) {
        updateScore(scores.fees, i);
      }

      if (text.includes('LIQUID') || text.includes('NET')) {
        updateScore(scores.net, i);
      }
    }
  });

  const pickBest = (map: Map<number, number>): number | null => {
    let bestIndex: number | null = null;
    let bestScore = 0;
    for (const [index, score] of map.entries()) {
      if (score > bestScore) {
        bestScore = score;
        bestIndex = index;
      }
    }
    return bestIndex;
  };

  return {
    valueIndex: pickBest(scores.value),
    brandIndex: pickBest(scores.brand),
    statusIndex: pickBest(scores.status),
    installmentsIndex: pickBest(scores.installments),
    typeIndex: pickBest(scores.type),
    nsuIndex: pickBest(scores.nsu),
    feesIndex: pickBest(scores.fees),
    netIndex: pickBest(scores.net),
  };
};

const parseDateCell = (value: unknown): Date | null => {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value;
  }
  if (typeof value === 'number') {
    const date = xlsx.SSF ? xlsx.SSF.parse_date_code(value) : null;
    if (date) {
      return new Date(date.y, date.m - 1, date.d, date.H, date.M, date.S);
    }
  }
  if (typeof value === 'string') {
    const text = value.trim();
    if (!text) {
      return null;
    }

    // Prefer explicit pt-BR parsing to avoid locale-dependent Date parsing.
    // Examples: 05/01/2026, 05/01/2026 06:36[:59]
    const br = text.match(
      /^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/,
    );
    if (br) {
      const day = Number(br[1]);
      const month = Number(br[2]);
      const year = Number(br[3]);
      const hour = br[4] ? Number(br[4]) : 0;
      const minute = br[5] ? Number(br[5]) : 0;
      const second = br[6] ? Number(br[6]) : 0;
      const parsed = new Date(year, month - 1, day, hour, minute, second);
      if (!Number.isNaN(parsed.getTime())) {
        return parsed;
      }
    }

    // ISO-like yyyy-mm-dd with optional time.
    const iso = text.match(
      /^(\d{4})-(\d{1,2})-(\d{1,2})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/,
    );
    if (iso) {
      const year = Number(iso[1]);
      const month = Number(iso[2]);
      const day = Number(iso[3]);
      const hour = iso[4] ? Number(iso[4]) : 0;
      const minute = iso[5] ? Number(iso[5]) : 0;
      const second = iso[6] ? Number(iso[6]) : 0;
      const parsed = new Date(year, month - 1, day, hour, minute, second);
      if (!Number.isNaN(parsed.getTime())) {
        return parsed;
      }
    }
  }
  if (isValidDate(value)) {
    const parsed = new Date(String(value));
    if (!Number.isNaN(parsed.getTime())) {
      return parsed;
    }
  }
  return null;
};

const applyTime = (date: Date, timeValue: unknown): Date => {
  if (typeof timeValue === 'number') {
    const totalSeconds = Math.round(timeValue * 24 * 60 * 60);
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    return new Date(date.getFullYear(), date.getMonth(), date.getDate(), hours, minutes, seconds);
  }
  const text = normalizeText(timeValue);
  const match = text.match(/(\d{1,2}):(\d{2})(?::(\d{2}))?/);
  if (match) {
    const hours = Number(match[1]);
    const minutes = Number(match[2]);
    const seconds = match[3] ? Number(match[3]) : 0;
    return new Date(date.getFullYear(), date.getMonth(), date.getDate(), hours, minutes, seconds);
  }
  return date;
};

const parseNamedColumns = (
  rows: unknown[][],
  headerIndex: number,
  mapping: Record<string, number | null>,
  source: string,
): ParseResult => {
  const sales: NormalizedSale[] = [];
  const invalidSales: NormalizedSale[] = [];
  let skippedRows = 0;
  let invalidRows = 0;

  for (let i = headerIndex + 1; i < rows.length; i += 1) {
    const row = rows[i];
    if (!row || row.length === 0) {
      skippedRows += 1;
      continue;
    }

    const saleNo = mapping.saleNoIndex !== null ? normalizeText(row[mapping.saleNoIndex]) : '';
    const total = mapping.totalIndex !== null ? parseMoney(row[mapping.totalIndex]) : null;

    const dateValue = mapping.dateIndex !== null ? row[mapping.dateIndex] : null;
    const fallbackDateValue =
      mapping.dateFallbackIndex !== null ? row[mapping.dateFallbackIndex] : null;
    const timeValue = mapping.timeIndex !== null ? row[mapping.timeIndex] : null;
    const baseDate = parseDateCell(dateValue) ?? parseDateCell(fallbackDateValue);
    const saleDate = baseDate ? (timeValue ? applyTime(baseDate, timeValue) : baseDate) : null;

    const paymentRaw = mapping.paymentIndex !== null ? normalizeText(row[mapping.paymentIndex]) : '';
    const brandRaw = mapping.brandIndex !== null ? normalizeText(row[mapping.brandIndex]) : '';
    const acquirerRaw = mapping.acquirerIndex !== null ? normalizeText(row[mapping.acquirerIndex]) : '';
    const statusRaw = mapping.statusIndex !== null ? normalizeText(row[mapping.statusIndex]) : null;
    const installments = mapping.installmentsIndex !== null ? parseInstallments(row[mapping.installmentsIndex]) : null;
    const authNsu = mapping.nsuIndex !== null ? normalizeText(row[mapping.nsuIndex]) : null;
    const authCode = mapping.authIndex !== null ? normalizeText(row[mapping.authIndex]) : null;
    const tid = mapping.tidIndex !== null ? normalizeText(row[mapping.tidIndex]) : null;
    const feesAmount = mapping.feesIndex !== null ? parseMoney(row[mapping.feesIndex]) : null;
    const netAmount = mapping.netIndex !== null ? parseMoney(row[mapping.netIndex]) : null;

    if (isHeaderLikeRow([saleNo, authNsu, brandRaw, statusRaw])) {
      skippedRows += 1;
      continue;
    }

    const brandCandidate = brandRaw || acquirerRaw || paymentRaw || null;
    const paymentUpper = normalizeUpper(paymentRaw || brandCandidate || '');
    const paymentType = paymentUpper.includes('PIX') ? 'PIX' : paymentUpper ? 'CARD' : 'UNKNOWN';

    let cardMode: 'CREDIT' | 'DEBIT' | 'UNKNOWN' = 'UNKNOWN';
    if (paymentUpper.includes('DEBIT') || paymentUpper.includes('DEBITO')) {
      cardMode = 'DEBIT';
    } else if (paymentUpper.includes('CRED') || paymentUpper.includes('CREDITO')) {
      cardMode = 'CREDIT';
    }

    const isCancelled = statusRaw
      ? normalizeUpper(statusRaw).includes('CANCEL') || normalizeUpper(statusRaw).includes('ESTORN')
        ? 1
        : 0
      : 0;

    const rowHash = hashRow([
      source,
      saleNo,
      saleDate ? saleDate.toISOString() : '',
      authNsu,
      brandCandidate,
      total,
      statusRaw,
      `row-${i}`,
    ]);

    const sale: NormalizedSale = {
      source,
      saleNo,
      saleDatetime: saleDate,
      authNsu: authNsu || authCode || tid || null,
      cardBrandRaw: brandCandidate || null,
      paymentType,
      cardMode,
      installments,
      grossAmount: total,
      feesAmount,
      netAmount,
      statusRaw: statusRaw || null,
      isCancelled,
      rowHash,
    };

    if (total === null || !saleDate) {
      invalidRows += 1;
      const reasons: string[] = [];
      if (total === null) {
        reasons.push('GROSS_AMOUNT');
      }
      if (!saleDate) {
        reasons.push('SALE_DATETIME');
      }
      invalidSales.push({ ...sale, invalidReason: reasons.join(',') });
      continue;
    }

    sales.push(sale);
  }

  return {
    sales,
    invalidSales,
    meta: {
      totalRows: rows.length,
      parsedRows: sales.length,
      skippedRows,
      invalidRows,
      detectedColumns: {
        ...mapping,
      },
    },
  };
};

const parseLegacy = (rows: unknown[][], source: string): ParseResult => {
  const candidateRows = rows.filter((row) => row.length > 0 && parseDateCell(row[0]));
  const columns = detectColumnsLegacy(candidateRows);

  const sales: NormalizedSale[] = [];
  const invalidSales: NormalizedSale[] = [];
  let skippedRows = 0;
  let invalidRows = 0;

  for (const row of candidateRows) {
    const saleDate = parseDateCell(row[0]);
    if (!saleDate) {
      skippedRows += 1;
      continue;
    }

    const grossAmount = columns.valueIndex !== null ? parseMoney(row[columns.valueIndex]) : null;
    const brandRaw = columns.brandIndex !== null ? normalizeText(row[columns.brandIndex]) : null;
    const typeRaw = columns.typeIndex !== null ? normalizeUpper(row[columns.typeIndex]) : '';
    const statusRaw = columns.statusIndex !== null ? normalizeText(row[columns.statusIndex]) : null;
    const installments = columns.installmentsIndex !== null ? parseInstallments(row[columns.installmentsIndex]) : null;
    const authNsu = columns.nsuIndex !== null ? normalizeText(row[columns.nsuIndex]) : null;
    const feesAmount = columns.feesIndex !== null ? parseMoney(row[columns.feesIndex]) : null;
    const netAmount = columns.netIndex !== null ? parseMoney(row[columns.netIndex]) : null;

    const brandUpper = normalizeUpper(brandRaw);
    const paymentType = brandUpper.includes('PIX') ? 'PIX' : brandUpper ? 'CARD' : 'UNKNOWN';

    let cardMode: 'CREDIT' | 'DEBIT' | 'UNKNOWN' = 'UNKNOWN';
    if (typeRaw.includes('DEBIT')) {
      cardMode = 'DEBIT';
    } else if (typeRaw.includes('CRED')) {
      cardMode = 'CREDIT';
    } else if (brandUpper.includes('DEBIT')) {
      cardMode = 'DEBIT';
    } else if (brandUpper.includes('CRED')) {
      cardMode = 'CREDIT';
    }

    const isCancelled = statusRaw
      ? normalizeUpper(statusRaw).includes('CANCEL') || normalizeUpper(statusRaw).includes('ESTORN')
        ? 1
        : 0
      : 0;

    const rowHash = hashRow([
      source,
      saleDate.toISOString(),
      authNsu,
      brandRaw,
      grossAmount,
      statusRaw,
    ]);

    const saleNo = authNsu || `LEGACY-${rowHash.slice(0, 12)}`;

    const sale: NormalizedSale = {
      source,
      saleNo,
      saleDatetime: saleDate,
      authNsu: authNsu || null,
      cardBrandRaw: brandRaw || null,
      paymentType,
      cardMode,
      installments,
      grossAmount,
      feesAmount,
      netAmount,
      statusRaw: statusRaw || null,
      isCancelled,
      rowHash,
    };

    if (grossAmount === null) {
      invalidRows += 1;
      invalidSales.push({ ...sale, invalidReason: 'GROSS_AMOUNT' });
      continue;
    }

    sales.push(sale);
  }

  return {
    sales,
    invalidSales,
    meta: {
      totalRows: rows.length,
      parsedRows: sales.length,
      skippedRows,
      invalidRows,
      detectedColumns: {
        valueIndex: columns.valueIndex,
        brandIndex: columns.brandIndex,
        statusIndex: columns.statusIndex,
        installmentsIndex: columns.installmentsIndex,
        typeIndex: columns.typeIndex,
        nsuIndex: columns.nsuIndex,
        feesIndex: columns.feesIndex,
        netIndex: columns.netIndex,
      },
    },
  };
};

export const parseWorkbook = (buffer: Buffer, filename: string, source: string): ParseResult => {
  const workbook = xlsx.read(buffer, { type: 'buffer', cellDates: true });
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const rows = xlsx.utils.sheet_to_json<unknown[]>(sheet, { header: 1, raw: true });

  const headerResult = findHeaderRow(rows);
  if (headerResult) {
    return parseNamedColumns(rows, headerResult.index, headerResult.mapping, source);
  }

  return parseLegacy(rows, source);
};
