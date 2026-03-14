import { StandardRow, StandardSource } from './types';

export const toText = (value: unknown): string => {
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) {
      return '';
    }
    const lowered = trimmed.toLowerCase();
    if (lowered === 'null' || lowered === 'undefined') {
      return '';
    }
    return trimmed;
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString();
  }
  return '';
};

export const toTextAny = (value: unknown): string => {
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'string') {
    return value;
  }
  if (typeof value === 'number' || typeof value === 'bigint' || typeof value === 'boolean') {
    return String(value);
  }
  if (typeof value === 'object') {
    const maybeBuffer = value as { type?: string; data?: number[]; toString?: (...args: any[]) => string };
    if (typeof maybeBuffer.toString === 'function' && (value as any).constructor?.name === 'Buffer') {
      try {
        return maybeBuffer.toString('utf8');
      } catch {
        return '';
      }
    }
    if (maybeBuffer.type === 'Buffer' && Array.isArray(maybeBuffer.data)) {
      try {
        return String.fromCharCode(...maybeBuffer.data);
      } catch {
        return '';
      }
    }
  }
  return String(value);
};

export const parseMoney = (value: unknown): number | null => {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }
  const text = toText(value);
  if (!text) {
    return null;
  }
  let cleaned = text.replace(/[^0-9,.-]/g, '');
  if (!cleaned) {
    return null;
  }
  const hasComma = cleaned.includes(',');
  const hasDot = cleaned.includes('.');
  if (hasComma && hasDot) {
    cleaned = cleaned.replace(/\./g, '').replace(/,/g, '.');
  } else if (hasComma) {
    cleaned = cleaned.replace(/,/g, '.');
  }
  const parsed = Number(cleaned);
  return Number.isNaN(parsed) ? null : parsed;
};

export const formatMoneyBRL = (value: number | null): string => {
  if (value === null || value === undefined) {
    return '';
  }
  return new Intl.NumberFormat('pt-BR', {
    style: 'currency',
    currency: 'BRL',
  }).format(value);
};

export const formatDatetimeCompact = (value: unknown): string => {
  if (!value) {
    return '';
  }
  const date = value instanceof Date ? value : new Date(String(value));
  if (Number.isNaN(date.getTime())) {
    return '';
  }
  const pad = (num: number) => String(num).padStart(2, '0');
  return `${pad(date.getDate())}/${pad(date.getMonth() + 1)}/${date.getFullYear()} ${pad(
    date.getHours(),
  )}:${pad(date.getMinutes())}`;
};

const firstText = (...values: unknown[]): string => {
  for (const value of values) {
    const text = toText(value);
    if (text) {
      return text;
    }
  }
  return '';
};

const firstNonEmpty = (...values: unknown[]): string | null => {
  for (const value of values) {
    const text = toText(value);
    if (text) {
      return text;
    }
  }
  return null;
};

const firstTextAny = (...values: unknown[]): string => {
  for (const value of values) {
    const text = toTextAny(value);
    if (text) {
      return text;
    }
  }
  return '';
};

const normalizePayment = (...values: unknown[]): string => {
  const raw = firstText(...values);
  if (!raw) {
    return '';
  }
  const normalized = raw
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toUpperCase();
  if (normalized.includes('PIX')) {
    return 'PIX';
  }
  if (normalized.includes('DEB')) {
    return 'Débito';
  }
  if (normalized.includes('CRED')) {
    return 'Crédito';
  }
  return raw;
};

const normalizeAcquirerStatusText = (value: string): string => {
  const raw = value.trim();
  if (!raw) {
    return raw;
  }

  if (raw === '-') {
    return '?';
  }

  const normalized = raw
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();

  if (normalized === 'transacao processada') {
    return 'Aprovada';
  }

  if (
    normalized.startsWith('undone') ||
    normalized === 'unauthorized - 90' ||
    normalized === 'cancelado'
  ) {
    return 'Cancelado';
  }

  return raw;
};

const formatNsu = (pdv?: unknown, acq?: unknown): string => {
  const pdvText = toTextAny(pdv);
  const acqText = toTextAny(acq);
  if (pdvText && acqText) {
    return `PDV: ${pdvText} | Adq: ${acqText}`;
  }
  if (pdvText) {
    return `PDV: ${pdvText}`;
  }
  if (acqText) {
    return `Adq: ${acqText}`;
  }
  return '';
};

const sumMoney = (values: unknown[]): number | null => {
  let total = 0;
  let hasValue = false;
  for (const value of values) {
    const parsed = parseMoney(value);
    if (parsed !== null) {
      total += parsed;
      hasValue = true;
    }
  }
  return hasValue ? total : null;
};

const resolveFees = (explicitValues: unknown[], total?: unknown, net?: unknown): number | null => {
  const summed = sumMoney(explicitValues);
  if (summed !== null) {
    return summed;
  }
  const totalValue = parseMoney(total);
  const netValue = parseMoney(net);
  if (totalValue !== null && netValue !== null) {
    return totalValue - netValue;
  }
  return null;
};

const operatorLabelFromKey = (key: string): string => {
  if (key === 'cielo') {
    return 'Cielo';
  }
  if (key === 'sipag') {
    return 'Sipag';
  }
  return '';
};

const normalizeOperatorKey = (source: StandardSource, input: any): string => {
  const lowered = source.toLowerCase();
  if (lowered === 'cielo' || lowered === 'sipag') {
    return lowered;
  }
  if (lowered === 'reconciled') {
    const key = firstTextAny(
      input?.ACQ_PROVIDER,
      input?.acqProvider,
      input?.acq_provider,
      input?.ACQUIRER,
      input?.acquirer,
    );
    return key ? key.trim().toLowerCase() : '';
  }
  const key = firstText(input?.ACQUIRER, input?.OPERATOR, input?.OPERADORA);
  return key ? key.trim().toLowerCase() : '';
};

const normalizeOperatorText = (source: StandardSource, input: any, operatorKey: string): string => {
  const lowered = source.toLowerCase();
  const label = operatorLabelFromKey(operatorKey);
  if (label) {
    return label;
  }
  if (lowered === 'interdata') {
    return 'Interdata';
  }
  if (lowered === 'reconciled') {
    return 'Conciliação';
  }
  return firstText(input?.ACQUIRER, input?.OPERATOR, input?.OPERADORA, source);
};

export const normalizeStandardRow = (input: any, source: StandardSource): StandardRow => {
  const lowered = source.toLowerCase();
  // Defensive field picking across provider schemas.
  const saleDatetime = firstText(
    input?.SALE_DATETIME,
    input?.ACQ_SALE_DATETIME,
    input?.TRANSACTION_DATETIME,
    input?.CREATED_AT,
    input?.ENTRY_DATE,
    input?.SETTLEMENT_DATE,
  );
  const datetimeText = formatDatetimeCompact(saleDatetime);
  const saleNoText = firstText(
    input?.SALE_NO,
    input?.ACQ_SALE_NO,
    input?.SALE_ID,
    input?.ORDER_NUMBER,
    input?.INVOICE_NUMBER,
    input?.TID,
  );
  const reconciledSaleNoText = firstTextAny(
    input?.SALE_NO,
    input?.ACQ_SALE_NO,
    input?.SALE_ID,
    input?.ORDER_NUMBER,
    input?.INVOICE_NUMBER,
    input?.TID,
  );
  const operatorKey = normalizeOperatorKey(source, input);
  const operatorText = normalizeOperatorText(source, input, operatorKey);
  const canonMethodGroup = firstNonEmpty(input?.CANON_METHOD_GROUP, input?.canonMethodGroup);
  const canonMethod = firstNonEmpty(input?.CANON_METHOD, input?.canonMethod);
  const acqCanonMethodGroup = firstNonEmpty(input?.ACQ_CANON_METHOD_GROUP, input?.acqCanonMethodGroup);
  const acqCanonMethod = firstNonEmpty(input?.ACQ_CANON_METHOD, input?.acqCanonMethod);
  const paymentFallback = normalizePayment(
    input?.ACQ_PAYMENT_METHOD_RESOLVED,
    input?.ACQ_PAYMENT_METHOD,
    input?.ACQ_PAYMENT_TYPE,
    input?.ACQ_CARD_MODE,
    input?.ACQ_CARD_TYPE,
    input?.ACQ_CREDIT_DEBIT_IND,
    input?.PAYMENT_METHOD,
    input?.PAYMENT_TYPE,
    input?.CARD_MODE,
    input?.CARD_TYPE,
    input?.CREDIT_DEBIT_IND,
    input?.MODALITY,
  );
  const paymentFromCanon = (methodGroup?: string | null, method?: string | null): string => {
    if (methodGroup) {
      return methodGroup.toUpperCase() === 'CARD' ? 'CARTÃO' : methodGroup.toUpperCase();
    }
    if (method) {
      return method.toUpperCase() === 'CARD' ? 'CARTÃO' : method.toUpperCase();
    }
    return '';
  };
  const paymentGroupOnly = (value: string): string => {
    const normalized = value
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toUpperCase();
    if (!normalized) {
      return '';
    }
    return normalized.includes('PIX') ? 'PIX' : 'CARTÃO';
  };
  const paymentText =
    lowered === 'reconciled'
      ? paymentGroupOnly(
          paymentFromCanon(acqCanonMethodGroup, acqCanonMethod) ||
            paymentFromCanon(canonMethodGroup, canonMethod) ||
            paymentFallback,
        )
      : paymentFromCanon(canonMethodGroup, canonMethod) || paymentFallback;
  const brandText =
    lowered === 'reconciled'
      ? firstNonEmpty(input?.ACQ_CANON_BRAND, input?.acqCanonBrand, input?.CANON_BRAND, input?.canonBrand) ||
        firstTextAny(
          input?.ACQ_BRAND_RESOLVED,
          input?.ACQ_BRAND,
          input?.ACQ_CARD_BRAND,
          input?.ACQ_CARD_BRAND_RAW,
          input?.BRAND,
          input?.CARD_BRAND,
          input?.CARD_BRAND_RAW,
          input?.CARD_BRAND_NAME,
          input?.CARD_BRAND_DESC,
          input?.BRAND_DESC,
          input?.CARD_BRAND_DESCRIPTION,
        )
      : firstNonEmpty(input?.CANON_BRAND, input?.canonBrand) ||
        firstTextAny(
          input?.ACQ_BRAND,
          input?.ACQ_CARD_BRAND,
          input?.ACQ_CARD_BRAND_RAW,
          input?.BRAND,
          input?.CARD_BRAND,
          input?.CARD_BRAND_RAW,
          input?.CARD_BRAND_NAME,
          input?.CARD_BRAND_DESC,
          input?.BRAND_DESC,
          input?.CARD_BRAND_DESCRIPTION,
        );
  const statusText =
    lowered === 'interdata'
      ? 'Sem Conciliação'
      : lowered === 'reconciled'
        ? normalizeAcquirerStatusText(
            firstText(
              input?.ACQ_STATUS_RESOLVED,
              input?.ACQ_STATUS,
              input?.STATUS,
              input?.STATUS_RAW,
              input?.STATUS_DESC,
            ),
          )
        : normalizeAcquirerStatusText(firstText(input?.STATUS, input?.STATUS_RAW, input?.STATUS_DESC));
  const originText = firstTextAny(input?.SOURCE, input?.source);
  const reasonText = firstTextAny(input?.REASON, input?.reason);
  const notesText = firstTextAny(input?.NOTES, input?.notes);
  const matchLayer =
    typeof input?.MATCH_LAYER === 'number' ? input.MATCH_LAYER : Number(input?.MATCH_LAYER ?? NaN);
  const matchConfidence =
    typeof input?.MATCH_CONFIDENCE === 'number'
      ? input.MATCH_CONFIDENCE
      : Number(input?.MATCH_CONFIDENCE ?? NaN);
  const matchReason = firstTextAny(input?.MATCH_REASON, input?.matchReason);

  const totalRaw =
    parseMoney(input?.GROSS_AMOUNT) ??
    parseMoney(input?.ACQ_GROSS_AMOUNT) ??
    parseMoney(input?.TOTAL_AMOUNT) ??
    parseMoney(input?.AMOUNT_TOTAL) ??
    parseMoney(input?.ORIGIN_VALUE);
  const netRaw =
    parseMoney(input?.NET_AMOUNT) ??
    parseMoney(input?.ACQ_NET_AMOUNT) ??
    parseMoney(input?.LIQUIDO) ??
    parseMoney(input?.AMOUNT_NET) ??
    parseMoney(input?.NET_VALUE);

  const feesRaw = resolveFees(
    [
      input?.FEES_AMOUNT,
      input?.FEE_AMOUNT,
      input?.AMOUNT_FEES,
      input?.TOTAL_FEES,
      input?.MDR_AMOUNT,
      input?.TERM_AMOUNT,
      input?.DESCONTO,
      input?.DESCONTO_VALOR,
      input?.DISCOUNT_AMOUNT,
      input?.ACRESCIMO,
      input?.ACRESCIMO_VALOR,
      input?.SURCHARGE_AMOUNT,
      input?.TAXA,
      input?.FEE,
    ],
    totalRaw ?? input?.GROSS_AMOUNT ?? input?.TOTAL_AMOUNT,
    netRaw ?? input?.NET_AMOUNT,
  );

  const canonMethodGroupUpper = canonMethodGroup ? canonMethodGroup.toUpperCase() : '';
  const canonMethodUpper = canonMethod ? canonMethod.toUpperCase() : '';
  const paymentUpper = paymentText ? paymentText.toUpperCase() : '';
  let nsuPreferred: string | null = null;
  if (canonMethodGroupUpper === 'CARD') {
    nsuPreferred = firstNonEmpty(input?.CANON_TERMINAL_NO, input?.canonTerminalNo);
  } else if (canonMethodGroupUpper === 'PIX') {
    nsuPreferred = firstNonEmpty(
      input?.CANON_NSU,
      input?.NSU_DOC,
      input?.E_NSU_DOC,
      input?.canonNsu,
    );
  } else if (!canonMethodGroupUpper && canonMethodUpper === 'PIX') {
    nsuPreferred = firstNonEmpty(
      input?.CANON_NSU,
      input?.NSU_DOC,
      input?.E_NSU_DOC,
      input?.canonNsu,
    );
  } else if (!canonMethodGroupUpper && canonMethodUpper) {
    nsuPreferred = firstNonEmpty(input?.CANON_TERMINAL_NO, input?.canonTerminalNo);
  } else if (paymentUpper === 'PIX') {
    nsuPreferred = firstNonEmpty(
      input?.CANON_NSU,
      input?.NSU_DOC,
      input?.E_NSU_DOC,
      input?.canonNsu,
    );
  }

  let nsuText = '';
  if (nsuPreferred) {
    nsuText = nsuPreferred.includes('PDV:') ? nsuPreferred : formatNsu(nsuPreferred);
  } else if (lowered === 'reconciled') {
    nsuText = formatNsu(input?.AUTH_NSU, input?.ACQ_NSU ?? input?.ACQ_AUTH_CODE);
  } else if (lowered === 'interdata') {
    nsuText = formatNsu(
      input?.AUTH_NSU ?? input?.NSU_PDV ?? input?.NSU,
      input?.ACQ_NSU ?? input?.NSU_ADQ,
    );
  } else {
    nsuText = formatNsu(
      input?.AUTH_CODE ?? input?.AUTH_NO ?? input?.TRANSACTION_NO ?? input?.NSU_DOC ?? input?.NSU,
      input?.ACQ_NSU ?? input?.ACQ_AUTH_CODE,
    );
  }

  const datetimeFallback =
    lowered === 'interdata'
      ? formatDatetimeCompact(firstText(input?.SALE_DATETIME, input?.CREATED_AT))
      : datetimeText;

  return {
    datetimeText: datetimeFallback,
    saleNoText: lowered === 'reconciled' ? reconciledSaleNoText : saleNoText,
    operatorText,
    operatorKey,
    paymentText,
    brandText,
    nsuText,
    statusText,
    originText,
    reasonText,
    notesText,
    totalText: formatMoneyBRL(totalRaw),
    feesText: formatMoneyBRL(feesRaw),
    netText: formatMoneyBRL(netRaw),
    matchLayer: Number.isNaN(matchLayer) ? null : matchLayer,
    matchConfidence: Number.isNaN(matchConfidence) ? null : matchConfidence,
    matchReason: matchReason || null,
  };
};
