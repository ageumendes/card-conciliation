import { Cron } from '@nestjs/schedule';
import { Injectable, Logger } from '@nestjs/common';
import { ReconciliationService } from '../reconciliation/reconciliation.service';
import { InterdataProgressService } from '../../integrations/interdata-import/interdata-progress.service';
import { RemoteEdiService } from '../../integrations/remote-edi/remote-edi.service';

@Injectable()
export class ReconciliationJob {
  private readonly logger = new Logger(ReconciliationJob.name);

  constructor(
    private readonly reconciliationService: ReconciliationService,
    private readonly progressService: InterdataProgressService,
    private readonly remoteEdiService: RemoteEdiService,
  ) {}

  @Cron('0 * * * *')
  async handleHourly() {
    await this.runIfNeeded('cron');
  }

  async runIfNeeded(source: string) {
    const yesterday = this.getYesterdayDateString();
    const runId = this.createRunId();
    if (this.progressService.isBusy()) {
      this.logger.log(`Conciliacao adiada (${source}) runId=${runId}: importacao em andamento`);
      return;
    }

    const hasPending = await this.reconciliationService.hasPendingInterdataSales({
      dateFrom: yesterday,
      dateTo: yesterday,
    });
    if (!hasPending) {
      this.logger.log(`Conciliacao ignorada (${source}) runId=${runId}: sem pendencias para ${yesterday}`);
      return;
    }

    const preSync = await this.syncRemoteBeforeReconciliation(runId, source, yesterday, yesterday);
    this.logger.log(`Conciliacao automatica iniciada (${source}) runId=${runId} date=${yesterday}`);
    const result = await this.reconciliationService.reconcile({
      limit: 10000,
      dateFrom: yesterday,
      dateTo: yesterday,
    });
    this.logger.log(`Conciliacao automatica finalizada (${source}) runId=${runId} date=${yesterday}`);
    this.logExecutionSummary(runId, source, yesterday, yesterday, 10000, preSync, result);
  }

  private getYesterdayDateString(): string {
    const date = new Date();
    date.setDate(date.getDate() - 1);
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  private async syncRemoteBeforeReconciliation(
    runId: string,
    source: string,
    dateFrom: string,
    dateTo: string,
  ): Promise<{ downloaded: number; imported: number; ok: boolean }> {
    const startedAt = Date.now();
    this.logger.log(
      `Pre-sync remoto iniciado (${source}) runId=${runId} dateFrom=${dateFrom} dateTo=${dateTo}`,
    );
    try {
      const pull = await this.remoteEdiService.pull({
        cielo: true,
        sipag: true,
        sicredi: true,
        dryRun: false,
        moveUnknownToError: false,
      });
      const imported = await this.remoteEdiService.importLocal({
        cielo: true,
        sipag: true,
        sicredi: true,
      });
      const elapsedMs = Date.now() - startedAt;
      this.logger.log(
        `Pre-sync remoto finalizado (${source}) runId=${runId} em ${elapsedMs}ms baixados=${pull.summary.total.downloaded} importados=${imported.summary.totalImported}`,
      );
      return {
        downloaded: pull.summary.total.downloaded,
        imported: imported.summary.totalImported,
        ok: pull.ok && imported.ok,
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const elapsedMs = Date.now() - startedAt;
      this.logger.warn(
        `Pre-sync remoto falhou (${source}) runId=${runId} em ${elapsedMs}ms motivo=${message}. Conciliacao seguira sem bloquear.`,
      );
      return { downloaded: 0, imported: 0, ok: false };
    }
  }

  private logExecutionSummary(
    runId: string,
    source: string,
    dateFrom: string,
    dateTo: string,
    limit: number,
    preSync: { downloaded: number; imported: number; ok: boolean },
    result: any,
  ) {
    const summary = result?.data ?? {};
    this.logger.log(
      `[AUTO-RECON SUMMARY] runId=${runId} source=${source} dateFrom=${dateFrom} dateTo=${dateTo} limit=${limit} remoteOk=${preSync.ok} remoteDownloaded=${preSync.downloaded} remoteImported=${preSync.imported} processed=${summary.processed ?? 0} matched=${summary.matched ?? 0} insertedRecon=${summary.insertedRecon ?? 0} pending=${summary.pending ?? 0} errors=${summary.errors ?? 0}`,
    );
  }

  private createRunId(): string {
    return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
  }
}
