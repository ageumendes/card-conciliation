import { dlog } from './debug';

export const isBufferLike = (value: unknown): value is { type?: string; data?: unknown } => {
  if (!value || typeof value !== 'object') {
    return false;
  }
  if (value instanceof Uint8Array || value instanceof ArrayBuffer) {
    return true;
  }
  const maybe = value as { type?: string; data?: unknown };
  return maybe.type === 'Buffer' && Array.isArray(maybe.data);
};

const truncate = (value: string, max = 500) => {
  if (value.length <= max) {
    return value;
  }
  return `${value.slice(0, max)}...`;
};

export const toText = (value: unknown): string => {
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'string') {
    return value;
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString();
  }
  if (value instanceof Uint8Array) {
    return new TextDecoder('utf-8').decode(value);
  }
  if (value instanceof ArrayBuffer) {
    return new TextDecoder('utf-8').decode(new Uint8Array(value));
  }
  if (typeof value === 'object' && (value as any)?.type === 'Buffer') {
    return '';
  }
  if (isBufferLike(value)) {
    try {
      const data = Array.isArray((value as any).data) ? (value as any).data : [];
      const bytes = new Uint8Array(data);
      return new TextDecoder('utf-8').decode(bytes);
    } catch {
      return '';
    }
  }
  try {
    const json = JSON.stringify(value);
    dlog('normalize.toText: objeto convertido', value);
    return truncate(json);
  } catch {
    return '[object]';
  }
};

export const toUpperSafe = (value: unknown): string => {
  return toText(value).toUpperCase();
};

export const toNumberSafe = (value: unknown): number | null => {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  if (typeof value === 'number' && !Number.isNaN(value)) {
    return value;
  }
  const text = toText(value);
  if (!text) {
    return null;
  }
  const normalized = text.replace(/\\s/g, '').replace('R$', '').replace(/\\./g, '').replace(',', '.');
  const parsed = Number(normalized);
  return Number.isNaN(parsed) ? null : parsed;
};
