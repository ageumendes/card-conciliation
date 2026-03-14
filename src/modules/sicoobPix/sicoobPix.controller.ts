import {
  BadRequestException,
  Controller,
  Get,
  HttpException,
  Logger,
  Post,
  Query,
  UseGuards,
} from '@nestjs/common';
import { SicoobPixService } from './sicoobPix.service';
import { SicoobPixTokenService } from './sicoobPix.token.service';
import { AdminGuard } from '../../auth/admin.guard';

@Controller('admin/sicoob-pix')
@UseGuards(AdminGuard)
export class SicoobPixController {
  private readonly logger = new Logger(SicoobPixController.name);

  constructor(
    private readonly sicoobPixService: SicoobPixService,
    private readonly tokenService: SicoobPixTokenService,
  ) {}

  @Post('import')
  async importDminus1(@Query('date') date?: string) {
    if (!date) {
      throw new BadRequestException('Parametro date (YYYY-MM-DD) e obrigatorio');
    }

    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      throw new BadRequestException('Parametro date deve estar no formato YYYY-MM-DD');
    }

    try {
      this.logger.log(`Importacao Pix Sicoob: ${date}`);
      const result = await this.sicoobPixService.importDminus1(date);
      return {
        ok: true,
        imported: result.imported,
        rawId: result.rawId,
      };
    } catch (error) {
      const status = (error as any)?.response?.status ?? 500;
      const message =
        (error as any)?.response?.data?.message ??
        (error as Error)?.message ??
        'Erro ao importar Pix Sicoob';
      this.logger.error(`Falha import Pix Sicoob: ${message}`);
      throw new HttpException(message, status);
    }
  }

  @Get('ping')
  async ping() {
    await this.tokenService.getAccessToken();
    return { ok: true };
  }
}
