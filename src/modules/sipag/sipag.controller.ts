import {
  BadRequestException,
  Controller,
  HttpException,
  Logger,
  Post,
  Query,
  UseGuards,
} from '@nestjs/common';
import { SipagService } from './sipag.service';
import { AdminGuard } from '../../auth/admin.guard';

@Controller('admin/sipag')
@UseGuards(AdminGuard)
export class SipagController {
  private readonly logger = new Logger(SipagController.name);

  constructor(private readonly sipagService: SipagService) {}

  @Post('import')
  async importDminus1(@Query('date') date?: string) {
    if (!date) {
      throw new BadRequestException('Parametro date (YYYY-MM-DD) e obrigatorio');
    }

    try {
      this.logger.log(`Importacao manual Sipag: ${date}`);
      const result = await this.sipagService.importDminus1(date);
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
        'Erro ao importar Sipag';
      this.logger.error(`Falha import Sipag: ${message}`);
      throw new HttpException(message, status);
    }
  }
}
