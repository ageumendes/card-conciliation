import { MethodGroup } from './types';

export function normalizeMethodGroup(value: any): MethodGroup {
  const s = String(value ?? '').trim().toUpperCase();
  if (!s) return 'OTHER';
  if (s.includes('PIX')) return 'PIX';
  if (s.includes('TEF')) return 'TEF';
  if (s.includes('DINHEIRO') || s.includes('CASH')) return 'CASH';
  if (
    s.includes('CARD') ||
    s.includes('CRED') ||
    s.includes('DEB') ||
    s.includes('VISA') ||
    s.includes('MASTER') ||
    s.includes('ELO') ||
    s.includes('AMEX') ||
    s.includes('HIPER')
  )
    return 'CARD';
  return 'OTHER';
}
