import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import * as chokidar from 'chokidar';
import type { FSWatcher } from 'chokidar';
import * as path from 'path';
import { InterdataImportService } from './interdata-import.service';
import { InterdataProgressService } from './interdata-progress.service';
import { ReconciliationService } from '../../modules/reconciliation/reconciliation.service';
import { RemoteEdiService } from '../remote-edi/remote-edi.service';

@Injectable()
export class InterdataImportWatcher implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(InterdataImportWatcher.name);
  private watcher?: FSWatcher;
  private debounceTimer?: NodeJS.Timeout;
  private isRunning = false;
  private pending = false;

  constructor(
    private readonly interdataService: InterdataImportService,
    private readonly progressService: InterdataProgressService,
    private readonly reconciliationService: ReconciliationService,
    private readonly remoteEdiService: RemoteEdiService,
  ) {}

  async onModuleInit(): Promise<void> {
    if (!this.interdataService.getEnabled() || !this.interdataService.getWatchEnabled()) {
      return;
    }

    await this.interdataService.ensureConfiguredDirs();
    await this.startWatcher();
  }

  async onModuleDestroy(): Promise<void> {
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
      this.debounceTimer = undefined;
    }

    if (this.watcher) {
      await this.watcher.close();
      this.watcher = undefined;
    }
  }

  private async startWatcher(): Promise<void> {
    const watchDir = path.resolve(process.cwd(), this.interdataService.getDirectories().dropDir);

    this.watcher = chokidar.watch(watchDir, {
      ignoreInitial: true,
      awaitWriteFinish: {
        stabilityThreshold: this.interdataService.getWatchStableMs(),
        pollInterval: 200,
      },
    });

    this.watcher.on('add', (filePath) => {
      this.logger.log(`ERP watcher detectou novo arquivo: ${path.basename(filePath)}`);
      this.scheduleScan();
    });

    this.watcher.on('change', (filePath) => {
      this.logger.log(`ERP watcher detectou alteracao: ${path.basename(filePath)}`);
      this.scheduleScan();
    });

    this.watcher.on('error', (error) => {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`ERP watcher erro: ${message}`);
    });

    this.logger.log(`ERP watcher ativo em ${watchDir}`);
  }

  private scheduleScan(): void {
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
    }

    this.debounceTimer = setTimeout(() => {
      this.debounceTimer = undefined;
      void this.runScan();
    }, this.interdataService.getWatchDebounceMs());
  }

  private async runScan(): Promise<void> {
    if (this.progressService.isBusy()) {
      this.pending = true;
      this.logger.log('ERP watcher aguardando fim de importacao manual/upload para iniciar scan');
      this.scheduleScan();
      return;
    }

    if (this.isRunning) {
      this.pending = true;
      this.logger.log('ERP watcher scan coalescido (pending)');
      return;
    }

    this.isRunning = true;
    try {
      this.progressService.startManualImport();
      const result = await this.interdataService.scanDropDir();
      this.logger.log(
        `ERP watcher scan finalizado: processedFiles=${result.processedFiles} insertedSales=${result.insertedSales} skippedDuplicates=${result.skippedDuplicates} invalidRows=${result.invalidRows} errors=${result.errors}`,
      );
      if (result.insertedSales > 0) {
        await this.runReconciliationIfNeeded(
          result.reconciliationDateFrom ?? undefined,
          result.reconciliationDateTo ?? undefined,
        );
      } else {
        this.logger.log(
          'ERP watcher nao disparou conciliacao: scan sem novas vendas inseridas',
        );
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`ERP watcher falha no scan: ${message}`);
    } finally {
      this.progressService.endManualImport();
      this.isRunning = false;
      if (this.pending) {
        this.pending = false;
        this.scheduleScan();
      }
    }
  }

  private async runReconciliationIfNeeded(dateFrom?: string, dateTo?: string): Promise<void> {
    const runId = this.createRunId();
    const hasPending = await this.reconciliationService.hasPendingInterdataSales({
      dateFrom,
      dateTo,
    });
    if (!hasPending) {
      return;
    }

    try {
      const preSync = await this.syncRemoteBeforeReconciliation(runId, dateFrom, dateTo);
      const result = await this.reconciliationService.reconcile({ limit: 10000, dateFrom, dateTo });
      this.logger.log(
        `ERP watcher disparou conciliacao automatica apos importacao runId=${runId} dateFrom=${dateFrom ?? 'N/A'} dateTo=${dateTo ?? 'N/A'}`,
      );
      this.logExecutionSummary(runId, dateFrom, dateTo, 10000, preSync, result);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (message.includes('reconciliation_already_running')) {
        this.logger.warn('ERP watcher nao iniciou conciliacao: reconciliation_already_running');
        return;
      }
      this.logger.error(`ERP watcher falha ao disparar conciliacao: ${message}`);
    }
  }

  private async syncRemoteBeforeReconciliation(
    runId: string,
    dateFrom?: string,
    dateTo?: string,
  ): Promise<{ downloaded: number; imported: number; ok: boolean }> {
    const startedAt = Date.now();
    this.logger.log(
      `ERP watcher pre-sync remoto iniciado runId=${runId} dateFrom=${dateFrom ?? 'N/A'} dateTo=${dateTo ?? 'N/A'}`,
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
        `ERP watcher pre-sync remoto finalizado runId=${runId} em ${elapsedMs}ms baixados=${pull.summary.total.downloaded} importados=${imported.summary.totalImported}`,
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
        `ERP watcher pre-sync remoto falhou runId=${runId} em ${elapsedMs}ms motivo=${message}. Conciliacao seguira sem bloquear.`,
      );
      return { downloaded: 0, imported: 0, ok: false };
    }
  }

  private logExecutionSummary(
    runId: string,
    dateFrom: string | undefined,
    dateTo: string | undefined,
    limit: number,
    preSync: { downloaded: number; imported: number; ok: boolean },
    result: any,
  ) {
    const summary = result?.data ?? {};
    this.logger.log(
      `[AUTO-RECON SUMMARY] runId=${runId} source=erp-watcher dateFrom=${dateFrom ?? 'N/A'} dateTo=${dateTo ?? 'N/A'} limit=${limit} remoteOk=${preSync.ok} remoteDownloaded=${preSync.downloaded} remoteImported=${preSync.imported} processed=${summary.processed ?? 0} matched=${summary.matched ?? 0} insertedRecon=${summary.insertedRecon ?? 0} pending=${summary.pending ?? 0} errors=${summary.errors ?? 0}`,
    );
  }

  private createRunId(): string {
    return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
  }
}
