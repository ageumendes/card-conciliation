import { Controller, Get, Post, Query, UseGuards } from '@nestjs/common';
import { AdminGuard } from '../../auth/admin.guard';
import { LocalAcquirerCsvService } from './local-acquirer-csv.service';

@Controller('admin/local-csv')
@UseGuards(AdminGuard)
export class LocalAcquirerCsvController {
  constructor(private readonly localCsvService: LocalAcquirerCsvService) {}

  @Get('ping')
  ping() {
    return {
      ok: true,
      enabled: {
        sipag: this.localCsvService.getSipagEnabled(),
        sicredi: this.localCsvService.getSicrediEnabled(),
        sicrediEdi: this.localCsvService.getSicrediEdiEnabled(),
      },
      dirs: {
        sipag: this.localCsvService.getSipagDirs(),
        sicredi: this.localCsvService.getSicrediDirs(),
        sicrediEdi: this.localCsvService.getSicrediEdiDirs(),
      },
      debounceMs: this.localCsvService.getDebounceMs(),
      stableMs: this.localCsvService.getStableMs(),
    };
  }

  @Post('scan')
  async scan(@Query('sipag') sipag?: string, @Query('sicredi') sicredi?: string) {
    const runSipag = this.parseFlag(sipag, true);
    const runSicredi = this.parseFlag(sicredi, true);

    const sipagResult = runSipag ? await this.localCsvService.scanSipag() : null;
    const sicrediResult = runSicredi ? await this.localCsvService.scanSicredi() : null;
    const sicrediEdiResult = runSicredi ? await this.localCsvService.scanSicrediEdi() : null;

    return {
      ok: true,
      requested: {
        sipag: runSipag,
        sicredi: runSicredi,
      },
      sipag: sipagResult,
      sicredi: sicrediResult,
      sicrediEdi: sicrediEdiResult,
    };
  }

  private parseFlag(value: string | undefined, defaultValue: boolean): boolean {
    if (value == null || value === '') {
      return defaultValue;
    }

    const normalized = value.trim().toLowerCase();
    if (['1', 'true', 'yes', 'y', 'on'].includes(normalized)) {
      return true;
    }
    if (['0', 'false', 'no', 'n', 'off'].includes(normalized)) {
      return false;
    }
    return defaultValue;
  }
}
