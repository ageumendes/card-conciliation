import { BadRequestException, Body, Controller, Get, Logger, Post, Query, UseGuards } from '@nestjs/common';
import { CieloSftpEdiService } from './cielo-sftp-edi.service';
import { AdminGuard } from '../../auth/admin.guard';

@Controller('admin/cielo-edi')
@UseGuards(AdminGuard)
export class CieloSftpEdiController {
  private readonly logger = new Logger(CieloSftpEdiController.name);

  constructor(private readonly cieloEdiService: CieloSftpEdiService) {}

  @Get('ping')
  getPing() {
    const mode = this.cieloEdiService.getMode();
    const dirs = this.cieloEdiService.getDirectories();
    return { ok: true, mode, dirs };
  }

  @Get('files')
  async listFiles() {
    const files = await this.cieloEdiService.listLocalFiles();
    return { ok: true, files };
  }

  @Post('sync')
  async sync() {
    const result = await this.cieloEdiService.syncFromSftp();
    return { ok: true, ...result };
  }

  @Post('scan')
  async scan() {
    const result = await this.cieloEdiService.scanLocal();
    return { ok: true, ...result };
  }

  @Get('reprocess')
  async reprocess(@Query('from') from?: string, @Query('to') to?: string) {
    if (!from || !to || !/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to)) {
      throw new BadRequestException('from/to invalidos');
    }
    const result = await this.cieloEdiService.reprocessSales(from, to);
    return { ok: true, ...result };
  }

  @Post('ajustes/match')
  async matchAjustes(
    @Body() body: { fileName?: string; dryRun?: boolean },
    @Query('fileName') fileName?: string,
    @Query('dryRun') dryRun?: string,
  ) {
    const bodyFileName = typeof body?.fileName === 'string' ? body.fileName.trim() : '';
    const queryFileName = typeof fileName === 'string' ? fileName.trim() : '';
    const resolvedFileName = bodyFileName || queryFileName;
    if (!resolvedFileName) {
      throw new BadRequestException('fileName obrigatorio');
    }
    const dryRunEnabled =
      typeof body?.dryRun === 'boolean'
        ? body.dryRun
        : ['true', '1', 'yes'].includes((dryRun ?? '').toLowerCase());
    if (this.cieloEdiService.isVerboseEnabled()) {
      const source = bodyFileName ? 'body' : queryFileName ? 'query' : 'missing';
      this.logger.debug(
        `CIELO04 match params source=${source} fileName=${resolvedFileName} dryRun=${dryRunEnabled}`,
      );
    }
    const result = await this.cieloEdiService.matchCielo04AdjustmentItems({
      fileName: resolvedFileName,
      dryRun: dryRunEnabled,
    });
    return { ok: true, result };
  }

  @Post('ajustes/apply')
  async applyAjustes(
    @Body() body: { fileName?: string; dryRun?: boolean },
    @Query('fileName') fileName?: string,
    @Query('dryRun') dryRun?: string,
  ) {
    const bodyFileName = typeof body?.fileName === 'string' ? body.fileName.trim() : '';
    const queryFileName = typeof fileName === 'string' ? fileName.trim() : '';
    const resolvedFileName = bodyFileName || queryFileName;
    if (!resolvedFileName) {
      throw new BadRequestException('fileName obrigatorio');
    }
    const dryRunEnabled =
      typeof body?.dryRun === 'boolean'
        ? body.dryRun
        : ['true', '1', 'yes'].includes((dryRun ?? '').toLowerCase());
    if (this.cieloEdiService.isVerboseEnabled()) {
      const source = bodyFileName ? 'body' : queryFileName ? 'query' : 'missing';
      this.logger.debug(
        `CIELO04 apply params source=${source} fileName=${resolvedFileName} dryRun=${dryRunEnabled}`,
      );
    }
    const result = await this.cieloEdiService.applyCielo04AdjustmentItems({
      fileName: resolvedFileName,
      dryRun: dryRunEnabled,
    });
    return { ok: true, ...result };
  }
}
