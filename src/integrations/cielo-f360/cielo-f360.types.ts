export interface CieloF360LoginResponse {
  Token?: string;
  Mensagem?: string;
}

export interface CieloF360ContaBancaria {
  Id?: number | string;
  Banco?: string;
  Agencia?: string;
  Conta?: string;
  [key: string]: unknown;
}

export interface CieloF360ExtratoRequest {
  DataInicio: string;
  DataFim: string;
  Status?: string;
  ModeloRelatorio?: string;
  Contas?: string[];
  ExibirDetalhesConciliacao?: boolean;
  CNPJEmpresas?: string[];
  Pagina?: number;
  [key: string]: unknown;
}

export interface CieloF360ParcelasQuery {
  inicio: string;
  fim: string;
  pagina?: number;
  tipo?: string;
  tipoDatas?: string;
  status?: string;
  empresas?: string[];
  contas?: string[];
}

export interface CieloF360RelatorioRequest {
  DataInicio: string;
  DataFim: string;
  TipoConciliacao?: string;
  EnviarNotificacaoPorWebhook?: boolean;
  URLNotificacao?: string;
  CNPJEmpresas?: string[];
  [key: string]: unknown;
}
