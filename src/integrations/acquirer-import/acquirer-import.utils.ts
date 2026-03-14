import { createHash } from 'crypto';

export const normalizeText = (value: unknown): string => {
  if (value === null || value === undefined) {
    return '';
  }
  return String(value).trim();
};

export const normalizeHeader = (value: unknown): string => {
  const text = normalizeText(value).toLowerCase();
  const noAccent = text.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  return noAccent.replace(/[^a-z0-9]+/g, ' ').trim();
};

export const parseMoneyFlexible = (value: unknown): number | null => {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  if (typeof value === 'number' && !Number.isNaN(value)) {
    return Number(value.toFixed(2));
  }

  const raw = normalizeText(value);
  if (!raw) {
    return null;
  }

  const normalized = raw
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

export const parseMoneyDot = (value: unknown): number | null => {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  if (typeof value === 'number' && !Number.isNaN(value)) {
    return Number(value.toFixed(2));
  }
  const raw = normalizeText(value);
  if (!raw) {
    return null;
  }
  const normalized = raw.replace(/\s/g, '').replace('R$', '');
  const parsed = Number(normalized);
  if (Number.isNaN(parsed)) {
    return null;
  }
  return Number(parsed.toFixed(2));
};

export const parseCsvLine = (line: string, delimiter = ';'): string[] => {
  const result: string[] = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (char === delimiter && !inQuotes) {
      result.push(current);
      current = '';
      continue;
    }
    current += char;
  }
  result.push(current);
  return result.map((value) => value.trim());
};

export const parseDateTime = (value: string): Date | null => {
  const text = normalizeText(value);
  if (!text) {
    return null;
  }
  const match = text.match(/(\d{2})\/(\d{2})\/(\d{4})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?/);
  if (!match) {
    return null;
  }
  const day = Number(match[1]);
  const month = Number(match[2]);
  const year = Number(match[3]);
  const hours = match[4] ? Number(match[4]) : 0;
  const minutes = match[5] ? Number(match[5]) : 0;
  const seconds = match[6] ? Number(match[6]) : 0;
  const date = new Date(year, month - 1, day, hours, minutes, seconds);
  return Number.isNaN(date.getTime()) ? null : date;
};

export const parseDateIso = (value: string): Date | null => {
  const text = normalizeText(value);
  if (!text) {
    return null;
  }
  const match = text.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (!match) {
    return null;
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const date = new Date(year, month - 1, day, 0, 0, 0);
  return Number.isNaN(date.getTime()) ? null : date;
};

export const parseDateTimeParts = (dateValue: string, timeValue?: string): Date | null => {
  const dateText = normalizeText(dateValue);
  if (!dateText) {
    return null;
  }
  const timeText = normalizeText(timeValue);
  return parseDateTime(timeText ? `${dateText} ${timeText}` : dateText);
};

export const parsePercent = (value: unknown): number | null => {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  const raw = normalizeText(value).replace('%', '');
  return parseMoneyFlexible(raw);
};

export const hashRow = (parts: Array<string | number | null | undefined>): string => {
  const content = parts.map((part) => String(part ?? '')).join('|');
  return createHash('sha256').update(content).digest('hex');
};
