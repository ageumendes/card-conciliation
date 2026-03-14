import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Param,
  Post,
  Query,
  Res,
  UseGuards,
} from '@nestjs/common';
import { Response } from 'express';
import { CieloF360Service } from './cielo-f360.service';
import {
  CieloF360ExtratoRequest,
  CieloF360ParcelasQuery,
  CieloF360RelatorioRequest,
} from './cielo-f360.types';
import { AdminGuard } from '../../auth/admin.guard';

/**
 * Exemplos:
 * curl http://localhost:3000/admin/cielo-f360/ping
 * curl http://localhost:3000/admin/cielo-f360/contas
 * curl -X POST http://localhost:3000/admin/cielo-f360/extrato -H "Content-Type: application/json" -d '{"DataInicio":"2024-01-01","DataFim":"2024-01-31"}'
 * curl "http://localhost:3000/admin/cielo-f360/parcelas-cartoes?inicio=2024-01-01&fim=2024-01-31&pagina=1"
 * curl -X POST http://localhost:3000/admin/cielo-f360/relatorios/conciliacao-cartoes -H "Content-Type: application/json" -d '{"DataInicio":"2024-01-01","DataFim":"2024-01-31"}'
 */
@Controller('admin/cielo-f360')
@UseGuards(AdminGuard)
export class CieloF360Controller {
  constructor(private readonly cieloService: CieloF360Service) {}

  @Get('ping')
  async ping() {
    await this.cieloService.listContasBancarias();
    return { ok: true };
  }

  @Get('contas')
  async listarContas() {
    const data = await this.cieloService.listContasBancarias();
    return { ok: true, data };
  }

  @Post('extrato')
  async obterExtrato(@Body() body: CieloF360ExtratoRequest) {
    if (!body?.DataInicio || !body?.DataFim) {
      throw new BadRequestException('DataInicio e DataFim sao obrigatorios');
    }

    const data = await this.cieloService.obterExtratoBancario(body);
    return { ok: true, data };
  }

  @Get('parcelas-cartoes')
  async listParcelas(@Query() query: Record<string, string>) {
    if (!query.inicio || !query.fim) {
      throw new BadRequestException('inicio e fim sao obrigatorios');
    }

    const parsed: CieloF360ParcelasQuery = {
      inicio: query.inicio,
      fim: query.fim,
      pagina: query.pagina ? Number(query.pagina) : 1,
      tipo: query.tipo ?? 'ambos',
      tipoDatas: query.tipoDatas ?? 'emissao',
      status: query.status,
      empresas: query.empresas ? query.empresas.split(',') : undefined,
      contas: query.contas ? query.contas.split(',') : undefined,
    };

    const data = await this.cieloService.listParcelasCartoes(parsed);
    return { ok: true, data };
  }

  @Post('relatorios/conciliacao-cartoes')
  async gerarRelatorio(@Body() body: CieloF360RelatorioRequest) {
    if (!body?.DataInicio || !body?.DataFim) {
      throw new BadRequestException('DataInicio e DataFim sao obrigatorios');
    }

    const data = await this.cieloService.gerarRelatorioConciliacaoCartoes(body);
    return { ok: true, data };
  }

  @Get('relatorios/download/:id')
  async downloadRelatorio(
    @Param('id') id: string,
    @Res({ passthrough: true }) res: Response,
  ) {
    if (!id) {
      throw new BadRequestException('Id do relatorio e obrigatorio');
    }

    const buffer = await this.cieloService.downloadRelatorio(id);
    res.setHeader('Content-Type', 'application/octet-stream');
    return Buffer.from(buffer);
  }
}
