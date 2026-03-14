import { Cron } from '@nestjs/schedule';
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { DbService } from '../../db/db.service';
import { SicoobPixService } from '../sicoobPix/sicoobPix.service';

@Injectable()
export class SicoobPixJob {
  private readonly logger = new Logger(SicoobPixJob.name);

  constructor(
    private readonly sicoobPixService: SicoobPixService,
    private readonly dbService: DbService,
    private readonly configService: ConfigService,
  ) {}

  @Cron('40 6 * * *')
  async handleDaily() {
    if (!this.isJobEnabled()) {
      this.logger.log('Job Pix Sicoob desativado por configuracao (SICOOB_PIX_JOB_ENABLED=false)');
      return;
    }

    const refDate = this.getYesterdayDate();
    const startedAt = new Date();
    let status = 'SUCCESS';
    let errorMessage: string | null = null;

    try {
      this.logger.log(`Job Pix Sicoob D-1 iniciado: ${refDate}`);
      await this.sicoobPixService.importDminus1(refDate);
      this.logger.log(`Job Pix Sicoob D-1 finalizado: ${refDate}`);
    } catch (error) {
      status = 'ERROR';
      errorMessage = error instanceof Error ? error.message : 'Erro desconhecido';
      this.logger.error(`Job Pix Sicoob falhou: ${errorMessage}`);
    } finally {
      await this.registerRun(refDate, startedAt, new Date(), status, errorMessage);
    }
  }

  private async registerRun(
    refDate: string,
    startedAt: Date,
    finishedAt: Date,
    status: string,
    errorMessage: string | null,
  ) {
    const withErrorSql =
      'INSERT INTO JOB_RUNS (JOB_NAME, REF_DATE, STATUS, STARTED_AT, FINISHED_AT, ERROR_MESSAGE) VALUES (?, ?, ?, ?, ?, ?)';
    const withoutErrorSql =
      'INSERT INTO JOB_RUNS (JOB_NAME, REF_DATE, STATUS, STARTED_AT, FINISHED_AT) VALUES (?, ?, ?, ?, ?)';
    const fullParams = ['SICOOB_PIX_IMPORT_D-1', refDate, status, startedAt, finishedAt, errorMessage];
    const baseParams = ['SICOOB_PIX_IMPORT_D-1', refDate, status, startedAt, finishedAt];
    try {
      await this.dbService.execute(withErrorSql, fullParams);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'erro ao registrar JOB_RUNS';
      const missingErrorMessageColumn = String(message).toUpperCase().includes('COLUMN UNKNOWN')
        && String(message).toUpperCase().includes('ERROR_MESSAGE');
      if (missingErrorMessageColumn) {
        try {
          await this.dbService.execute(withoutErrorSql, baseParams);
          this.logger.warn(
            'JOB_RUNS sem coluna ERROR_MESSAGE: registro salvo sem detalhe de erro (fallback)',
          );
          return;
        } catch (fallbackError) {
          const fallbackMessage =
            fallbackError instanceof Error
              ? fallbackError.message
              : 'erro ao registrar JOB_RUNS sem ERROR_MESSAGE';
          this.logger.error(`Falha no fallback JOB_RUNS Pix Sicoob: ${fallbackMessage}`);
          return;
        }
      }
      this.logger.error(`Falha ao registrar JOB_RUNS Pix Sicoob: ${message}`);
    }
  }

  private isJobEnabled(): boolean {
    const raw = String(this.configService.get<string>('SICOOB_PIX_JOB_ENABLED') ?? 'false')
      .trim()
      .toLowerCase();
    return ['1', 'true', 'yes', 'on'].includes(raw);
  }

  private getYesterdayDate(): string {
    const date = new Date();
    date.setDate(date.getDate() - 1);
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }
}
