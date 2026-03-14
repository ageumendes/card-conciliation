import { createHash } from 'crypto';

export const parseMoney = (value: unknown): number | null => {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  if (typeof value === 'number' && !Number.isNaN(value)) {
    return Number(value.toFixed(2));
  }

  const str = String(value).trim();
  if (!str) {
    return null;
  }

  const normalized = str
    .replace(/\s/g, '')
    .replace('R$', '')
    .replace(/\./g, '')
    .replace(',', '.');

  const parsed = Number(normalized);
  if (Number.isNaN(parsed)) {
    return null;
  }

  return Number(parsed.toFixed(2));
};

export const parseInstallments = (value: unknown): number | null => {
  if (!value) {
    return null;
  }
  const match = String(value).match(/(\d+)\s*\/\s*(\d+)/);
  if (!match) {
    return null;
  }
  const total = Number(match[2]);
  return Number.isNaN(total) ? null : total;
};

export const hashRow = (parts: Array<string | number | null | undefined>): string => {
  const content = parts.map((part) => String(part ?? '')).join('|');
  return createHash('sha256').update(content).digest('hex');
};

export const isValidDate = (value: unknown): boolean => {
  if (!value) {
    return false;
  }
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return true;
  }
  const parsed = new Date(String(value));
  return !Number.isNaN(parsed.getTime());
};
