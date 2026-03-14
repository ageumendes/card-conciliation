import { CanonFlag } from '../canonical/types';

export const CANON_FLAG_LABEL: Record<CanonFlag, string> = {
  MATCH: 'Conciliado (bate dentro das tolerancias)',
  MISMATCH: 'Divergencia encontrada (valor/tempo/metodo/bandeira)',
  ERP_ONLY: 'Existe apenas no ERP',
  ACQ_ONLY: 'Existe apenas no adquirente',
  RECON_ONLY: 'Existe apenas na conciliacao',
  VALUE_TOLERANCE: 'Valor dentro da tolerancia',
  TIME_TOLERANCE: 'Horario dentro da tolerancia',
  METHOD_MISMATCH: 'Forma de pagamento diferente',
  BRAND_MISMATCH: 'Bandeira diferente',
};
