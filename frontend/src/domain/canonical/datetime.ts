export function toIsoSafe(input: any): string {
  if (!input) return new Date(0).toISOString();
  const d = input instanceof Date ? input : new Date(input);
  if (Number.isNaN(d.getTime())) return new Date(0).toISOString();
  return d.toISOString();
}

export function isoToDate(iso: string): string {
  return (iso || '').slice(0, 10) || '1970-01-01';
}

export function isoToTime(iso: string): string {
  return (iso || '').slice(11, 19) || '00:00:00';
}
