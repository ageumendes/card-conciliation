export interface SicoobPixRaw {
  endToEndId?: string;
  txid?: string;
  valor?: string | number;
  chave?: string;
  status?: string;
  horario?: string;
  pagador?: {
    cpfCnpj?: string;
    nome?: string;
  };
  devolucaoStatus?: string;
  [key: string]: unknown;
}

export interface SicoobPixResponse {
  pix?: SicoobPixRaw[];
  recebimentos?: SicoobPixRaw[];
  items?: SicoobPixRaw[];
  paginacao?: {
    paginaAtual?: number;
    totalPaginas?: number;
    proximaPagina?: number;
    hasMore?: boolean;
  };
  [key: string]: unknown;
}

export interface NormalizedPixTx {
  refDate: string;
  endToEndId?: string | null;
  txid?: string | null;
  valor?: number | null;
  chave?: string | null;
  status?: string | null;
  horario?: string | null;
  pagadorCpfCnpj?: string | null;
  pagadorNome?: string | null;
}
