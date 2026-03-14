import { NormalizedPixTx, SicoobPixRaw } from './sicoobPix.types';

const toNumber = (value?: string | number): number | null => {
  if (value === undefined || value === null || value === '') {
    return null;
  }
  const parsed = Number(value);
  return Number.isNaN(parsed) ? null : parsed;
};

export const mapSicoobPixToNormalized = (
  raw: SicoobPixRaw,
  refDate: string,
): NormalizedPixTx => {
  return {
    refDate,
    endToEndId: raw.endToEndId ?? (raw as any).endToEndId ?? null,
    txid: raw.txid ?? (raw as any).txid ?? null,
    valor: toNumber(raw.valor ?? (raw as any).valor),
    chave: raw.chave ?? (raw as any).chave ?? null,
    status: raw.status ?? (raw as any).status ?? null,
    horario: raw.horario ?? (raw as any).horario ?? null,
    pagadorCpfCnpj: raw.pagador?.cpfCnpj ?? (raw as any).cpfCnpj ?? null,
    pagadorNome: raw.pagador?.nome ?? (raw as any).nome ?? null,
  };
};
