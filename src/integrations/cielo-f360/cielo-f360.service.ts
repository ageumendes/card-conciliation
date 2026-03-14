import { Injectable, Logger } from '@nestjs/common';
import {
  CieloF360ExtratoRequest,
  CieloF360ParcelasQuery,
  CieloF360RelatorioRequest,
} from './cielo-f360.types';
import { CieloF360Client } from './cielo-f360.client';

@Injectable()
export class CieloF360Service {
  private readonly logger = new Logger(CieloF360Service.name);

  constructor(private readonly client: CieloF360Client) {}

  async listContasBancarias() {
    this.logger.log('Listando contas bancarias Cielo F360');
    return this.client.get('/ContaBancariaPublicAPI/ListarContasBancarias');
  }

  async obterExtratoBancario(body: CieloF360ExtratoRequest) {
    this.logger.log('Obtendo extrato bancario Cielo F360');
    return this.client.post('/ExtratoBancarioPublicAPI/ObterExtratoBancario', body);
  }

  async listParcelasCartoes(query: CieloF360ParcelasQuery) {
    const params = this.buildQueryParams(query);
    const queryString = params.toString();
    const url = `/ParcelasDeCartoesPublicAPI/ListarParcelasDeCartoes?${queryString}`;
    this.logger.log(`Listando parcelas cartoes Cielo F360: ${url}`);
    return this.client.get(url);
  }

  async gerarRelatorioConciliacaoCartoes(body: CieloF360RelatorioRequest) {
    this.logger.log('Gerando relatorio conciliacao cartoes Cielo F360');
    return this.client.post('/PublicRelatorioAPI/GerarRelatorioDeConciliacaoDeCartoes', body);
  }

  async downloadRelatorio(id: string) {
    const url = `/PublicRelatorioAPI/Download?id=${encodeURIComponent(id)}`;
    this.logger.log(`Download relatorio Cielo F360: ${url}`);
    return this.client.get<ArrayBuffer>(url, { responseType: 'arraybuffer' });
  }

  private buildQueryParams(query: CieloF360ParcelasQuery): URLSearchParams {
    const params = new URLSearchParams();
    params.set('inicio', query.inicio);
    params.set('fim', query.fim);
    params.set('pagina', String(query.pagina ?? 1));
    params.set('tipo', query.tipo ?? 'ambos');
    params.set('tipoDatas', query.tipoDatas ?? 'emissao');

    if (query.status) {
      params.set('status', query.status);
    }

    if (query.empresas?.length) {
      params.set('empresas', query.empresas.join(','));
    }

    if (query.contas?.length) {
      params.set('contas', query.contas.join(','));
    }

    return params;
  }
}
