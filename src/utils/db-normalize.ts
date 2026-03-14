export function bufferToText(value: any): string {
  if (value == null) return '';
  if (Buffer.isBuffer(value)) {
    return value.toString('utf8').trim();
  }
  if (value?.type === 'Buffer' && Array.isArray(value.data)) {
    return Buffer.from(value.data).toString('utf8').trim();
  }
  return String(value).trim();
}

export function normalizeDbValue<T>(value: T): T | string {
  if (Buffer.isBuffer(value)) {
    return bufferToText(value);
  }
  if ((value as any)?.type === 'Buffer' && Array.isArray((value as any).data)) {
    return bufferToText(value);
  }
  return value;
}

export function normalizeDbRow<T extends Record<string, any>>(row: T): T {
  const entries = Object.entries(row).map(([key, value]) => [key, normalizeDbValue(value)]);
  return Object.fromEntries(entries) as T;
}

export function normalizeDbRows<T extends Record<string, any>>(
  rows: T[] | T | null | undefined,
): T[] {
  if (rows == null) {
    return [];
  }
  const list = Array.isArray(rows) ? rows : [rows];
  return list.map((row) => normalizeDbRow(row));
}
