import { Cron } from '@nestjs/schedule';
import { Injectable, Logger } from '@nestjs/common';
import { SipagService } from '../sipag/sipag.service';
import { DbService } from '../../db/db.service';

@Injectable()
export class Dminus1Job {
  private readonly logger = new Logger(Dminus1Job.name);

  constructor(
    private readonly sipagService: SipagService,
    private readonly dbService: DbService,
  ) {}

  @Cron('30 6 * * *')
  async handleDaily() {
    const refDate = this.getYesterdayDate();
    const startedAt = new Date();
    let status = 'SUCCESS';
    let errorMessage: string | null = null;

    try {
      this.logger.log(`Job D-1 iniciado: ${refDate}`);
      await this.sipagService.fetchDminus1(refDate);
      this.logger.log(`Job D-1 finalizado com sucesso: ${refDate}`);
    } catch (error) {
      status = 'ERROR';
      errorMessage = error instanceof Error ? error.message : 'Erro desconhecido';
      this.logger.error(`Job D-1 falhou: ${errorMessage}`);
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
    const fullParams = ['sipag_dminus1', refDate, status, startedAt, finishedAt, errorMessage];
    const baseParams = ['sipag_dminus1', refDate, status, startedAt, finishedAt];
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
          this.logger.error(`Falha no fallback JOB_RUNS: ${fallbackMessage}`);
          return;
        }
      }
      this.logger.error(`Falha ao registrar JOB_RUNS: ${message}`);
    }
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
