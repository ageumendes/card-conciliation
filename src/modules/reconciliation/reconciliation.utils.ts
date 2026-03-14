export const amountDiff = (acqAmount?: number | null, interAmount?: number | null): number => {
  const a = typeof acqAmount === 'number' ? acqAmount : 0;
  const b = typeof interAmount === 'number' ? interAmount : 0;
  return Number((a - b).toFixed(2));
};

export const normalizePaymentMethod = (input: unknown): 'PIX' | 'CARD' | 'OTHER' | null => {
  if (input === null || input === undefined) {
    return null;
  }
  const raw = String(input).trim().toUpperCase();
  if (!raw) {
    return null;
  }
  const pixKeys = [
    'PIX',
    'QR',
    'QRCODE',
    'QR_CODE',
    'INSTANT',
    'TRANSFER',
    'PAGAMENTO_INSTANTANEO',
    'PIX_QR',
  ];
  if (pixKeys.some((key) => raw.includes(key))) {
    return 'PIX';
  }
  const cardKeys = ['CREDIT', 'DEBIT', 'CARTAO', 'CARD'];
  if (cardKeys.some((key) => raw.includes(key))) {
    return 'CARD';
  }
  return 'OTHER';
};

export const toCents = (amount?: number | null): number | null => {
  if (typeof amount !== 'number' || Number.isNaN(amount)) {
    return null;
  }
  return Math.round(amount * 100);
};

export const normalizeNsu = (nsu?: string | null): string | null => {
  if (!nsu) {
    return null;
  }
  const digits = String(nsu).replace(/\D/g, '').trim();
  return digits ? digits : null;
};

export const diffMinutes = (a: Date, b: Date): number => {
  return Math.round((a.getTime() - b.getTime()) / 60000);
};

const parsePositiveNumber = (value: string | undefined, fallback: number): number => {
  if (typeof value !== 'string') {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallback;
  }
  return parsed;
};

// 5 horas cobre atrasos de processamento e diferencas de horario entre sistemas.
export const TIME_TOLERANCE_HOURS = 5;
export const TIME_TOLERANCE_MS = TIME_TOLERANCE_HOURS * 60 * 60 * 1000;
export const PIX_TIME_TOLERANCE_MINUTES = parsePositiveNumber(
  process.env.PIX_TIME_TOLERANCE_MINUTES,
  90,
);
export const DEFAULT_AMOUNT_TOLERANCE = parsePositiveNumber(
  process.env.DEFAULT_AMOUNT_TOLERANCE,
  0.01,
);
export const AMOUNT_TOLERANCE = DEFAULT_AMOUNT_TOLERANCE;

export type PaymentMethod = 'PIX' | 'CARD' | 'UNKNOWN';
export type CardType = 'DEBIT' | 'CREDIT' | 'UNKNOWN';

export const hasTimeComponent = (value?: Date | null): boolean => {
  if (!value) {
    return false;
  }
  return !(value.getHours() === 0 && value.getMinutes() === 0 && value.getSeconds() === 0);
};

export const normalizePaymentDetails = (value?: unknown): { paymentMethod: PaymentMethod; cardType: CardType } => {
  if (value === null || value === undefined) {
    return { paymentMethod: 'UNKNOWN', cardType: 'UNKNOWN' };
  }
  const raw = String(value).trim().toUpperCase();
  if (!raw) {
    return { paymentMethod: 'UNKNOWN', cardType: 'UNKNOWN' };
  }
  const text = raw.normalize('NFD').replace(/[\u0300-\u036f]/g, '');

  if (text.includes('PIX') || text.includes('QR') || text.includes('CARTEIRA') || text.includes('WALLET')) {
    return { paymentMethod: 'PIX', cardType: 'UNKNOWN' };
  }

  let cardType: CardType = 'UNKNOWN';
  if (text.includes('DEB') || text.includes('DEBITO') || text.includes('DEBIT')) {
    cardType = 'DEBIT';
  }
  if (
    text.includes('CREDITO') ||
    text.includes('CREDIT') ||
    text.includes('CRED') ||
    text.includes('CRD') ||
    text.includes('PARC') ||
    text.includes('PARCEL')
  ) {
    cardType = 'CREDIT';
  }

  if (cardType !== 'UNKNOWN') {
    return { paymentMethod: 'CARD', cardType };
  }

  if (text.includes('CARD') || text.includes('CARTAO')) {
    return { paymentMethod: 'CARD', cardType: 'UNKNOWN' };
  }

  return { paymentMethod: 'UNKNOWN', cardType: 'UNKNOWN' };
};

export const normalizePaymentDetailsFromValues = (
  ...values: unknown[]
): { paymentMethod: PaymentMethod; cardType: CardType } => {
  let best: { paymentMethod: PaymentMethod; cardType: CardType } = {
    paymentMethod: 'UNKNOWN',
    cardType: 'UNKNOWN',
  };

  values.forEach((value) => {
    const normalized = normalizePaymentDetails(value);
    if (normalized.paymentMethod === 'UNKNOWN') {
      return;
    }
    if (best.paymentMethod === 'UNKNOWN') {
      best = normalized;
      return;
    }
    if (
      best.paymentMethod === 'CARD' &&
      best.cardType === 'UNKNOWN' &&
      normalized.paymentMethod === 'CARD' &&
      normalized.cardType !== 'UNKNOWN'
    ) {
      best = normalized;
    }
  });

  return best;
};

export const buildDayWindow = (base: Date) => {
  const start = new Date(base);
  start.setHours(0, 0, 0, 0);
  const end = new Date(base);
  end.setHours(23, 59, 59, 999);
  return { start, end };
};

export const buildTimeWindow = (base: Date) => {
  return {
    start: new Date(base.getTime() - TIME_TOLERANCE_MS),
    end: new Date(base.getTime() + TIME_TOLERANCE_MS),
  };
};

export const timeDiffMs = (left?: Date | null, right?: Date | null): number | null => {
  if (!left || !right) {
    return null;
  }
  return Math.abs(left.getTime() - right.getTime());
};

export const isWithinTimeTolerance = (left?: Date | null, right?: Date | null): boolean => {
  const diff = timeDiffMs(left, right);
  return diff !== null && diff <= TIME_TOLERANCE_MS;
};

export const isWithinAmountTolerance = (
  left?: number | null,
  right?: number | null,
  tolerance: number = AMOUNT_TOLERANCE,
): boolean => {
  const diff = Math.abs(amountDiff(left, right));
  return diff <= tolerance;
};

export const normalizeIdentifier = (value?: unknown): string | null => {
  if (value === null || value === undefined) {
    return null;
  }
  const normalized = String(value).replace(/\s+/g, '').trim();
  return normalized || null;
};

export const stripLeadingZeros = (value: string): string => {
  return value.replace(/^0+/, '') || '0';
};

export const normalizeBrand = (value?: unknown): string | null => {
  if (value === null || value === undefined) {
    return null;
  }
  const raw = String(value).trim().toUpperCase();
  if (!raw) {
    return null;
  }
  const noAccent = raw.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  if (noAccent.includes('MASTERCARD') || noAccent.includes('MASTER')) {
    return 'MASTER';
  }
  if (noAccent.includes('VISA')) {
    return 'VISA';
  }
  if (noAccent.includes('ELO')) {
    return 'ELO';
  }
  return noAccent;
};

export const normalizeMethodToCreditDebit = (
  value?: unknown,
): 'CREDIT' | 'DEBIT' | null => {
  if (value === null || value === undefined) {
    return null;
  }
  const text = String(value).trim().toUpperCase();
  if (!text) {
    return null;
  }
  if (
    text.includes('DEB') ||
    text.includes('DEBITO') ||
    text.includes('DEBIT')
  ) {
    return 'DEBIT';
  }
  if (
    text.includes('CREDIT') ||
    text.includes('CREDITO') ||
    text.includes('CRED') ||
    text.includes('CRD') ||
    text.includes('PARC') ||
    text.includes('PARCEL') ||
    text.includes('A PRAZO')
  ) {
    return 'CREDIT';
  }
  if (text.includes('A VISTA')) {
    return null;
  }
  return null;
};

export const diffSeconds = (left?: Date | null, right?: Date | null): number | null => {
  if (!left || !right) {
    return null;
  }
  return Math.abs(Math.round((left.getTime() - right.getTime()) / 1000));
};

export const isSameDay = (left?: Date | null, right?: Date | null): boolean => {
  if (!left || !right) {
    return false;
  }
  return (
    left.getFullYear() === right.getFullYear() &&
    left.getMonth() === right.getMonth() &&
    left.getDate() === right.getDate()
  );
};

export const isCancelledStatus = (status?: string | null): boolean => {
  const raw = Array.isArray(status) ? status[0] : status;
  const text = String(raw ?? '').trim().toUpperCase();
  if (!text) {
    return false;
  }
  return (
    text.includes('CANCEL') ||
    text.includes('UNDONE') ||
    text.includes('UNAUTHORIZED - 90') ||
    text.includes('ESTORN') ||
    text.includes('NEG') ||
    text.includes('DEVOL')
  );
};

export const toFbTimestampString = (date: Date): string => {
  const pad = (value: number) => String(value).padStart(2, '0');
  const year = date.getFullYear();
  const month = pad(date.getMonth() + 1);
  const day = pad(date.getDate());
  const hours = pad(date.getHours());
  const minutes = pad(date.getMinutes());
  const seconds = pad(date.getSeconds());
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
};

export const toFbDateString = (date: Date): string => {
  const pad = (value: number) => String(value).padStart(2, '0');
  const year = date.getFullYear();
  const month = pad(date.getMonth() + 1);
  const day = pad(date.getDate());
  return `${year}-${month}-${day}`;
};

export const parseBoolean = (value?: string): boolean | undefined => {
  if (value === undefined || value === null) {
    return undefined;
  }
  const normalized = String(value).trim().toLowerCase();
  if (normalized === '' || normalized === 'false' || normalized === '0') {
    return false;
  }
  if (normalized === 'true' || normalized === '1' || normalized === 'yes') {
    return true;
  }
  return undefined;
};
