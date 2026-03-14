import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import * as chokidar from 'chokidar';
import type { FSWatcher } from 'chokidar';
import { LocalAcquirerCsvService, LocalCsvScanSummary } from './local-acquirer-csv.service';

type AcquirerKind = 'SIPAG' | 'SICREDI' | 'SICREDI_EDI';

@Injectable()
export class LocalAcquirerCsvWatcher implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(LocalAcquirerCsvWatcher.name);
  private readonly watchers = new Map<AcquirerKind, FSWatcher>();
  private readonly debounceTimers = new Map<AcquirerKind, NodeJS.Timeout>();
  private readonly isRunning = new Map<AcquirerKind, boolean>();
  private readonly pending = new Map<AcquirerKind, boolean>();

  constructor(private readonly localCsvService: LocalAcquirerCsvService) {}

  async onModuleInit(): Promise<void> {
    await this.localCsvService.ensureAllConfiguredDirs();

    if (this.localCsvService.getSipagEnabled()) {
      await this.startWatcher('SIPAG');
    }

    if (this.localCsvService.getSicrediEnabled()) {
      await this.startWatcher('SICREDI');
    }

    if (this.localCsvService.getSicrediEdiEnabled()) {
      await this.startWatcher('SICREDI_EDI');
    }
  }

  async onModuleDestroy(): Promise<void> {
    for (const timer of this.debounceTimers.values()) {
      clearTimeout(timer);
    }
    this.debounceTimers.clear();

    const closes: Promise<void>[] = [];
    for (const watcher of this.watchers.values()) {
      closes.push(watcher.close());
    }
    this.watchers.clear();
    await Promise.all(closes);
  }

  private async startWatcher(kind: AcquirerKind): Promise<void> {
    const dirs =
      kind === 'SIPAG'
        ? this.localCsvService.getSipagDirs()
        : kind === 'SICREDI'
          ? this.localCsvService.getSicrediDirs()
          : this.localCsvService.getSicrediEdiDirs();

    const watcher = chokidar.watch(dirs.watchDir, {
      ignoreInitial: true,
      awaitWriteFinish: {
        stabilityThreshold: this.localCsvService.getStableMs(),
        pollInterval: 200,
      },
    });

    watcher.on('add', () => {
      this.scheduleScan(kind);
    });

    watcher.on('change', () => {
      this.scheduleScan(kind);
    });

    watcher.on('error', (error) => {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`${kind} watcher erro: ${message}`);
    });

    this.watchers.set(kind, watcher);
    this.logger.log(`${kind} watcher ativo em ${dirs.watchDir}`);
  }

  private scheduleScan(kind: AcquirerKind): void {
    const currentTimer = this.debounceTimers.get(kind);
    if (currentTimer) {
      clearTimeout(currentTimer);
    }

    const debounceMs = this.localCsvService.getDebounceMs();
    const timer = setTimeout(() => {
      this.debounceTimers.delete(kind);
      void this.runScan(kind);
    }, debounceMs);

    this.debounceTimers.set(kind, timer);
  }

  private async runScan(kind: AcquirerKind): Promise<void> {
    if (this.isRunning.get(kind)) {
      this.pending.set(kind, true);
      this.logger.log(`${kind} scan coalescido (pending)`);
      return;
    }

    this.isRunning.set(kind, true);

    try {
      const summary =
        kind === 'SIPAG'
          ? await this.localCsvService.scanSipag()
          : kind === 'SICREDI'
            ? await this.localCsvService.scanSicredi()
            : await this.localCsvService.scanSicrediEdi();
      this.logScanSummary(kind, summary);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`${kind} falha no scan: ${message}`);
    } finally {
      this.isRunning.set(kind, false);
      if (this.pending.get(kind)) {
        this.pending.set(kind, false);
        this.scheduleScan(kind);
      }
    }
  }

  private logScanSummary(kind: AcquirerKind, summary: LocalCsvScanSummary): void {
    this.logger.log(
      `${kind} scan finalizado: processedFiles=${summary.processedFiles} okFiles=${summary.okFiles} errorFiles=${summary.errorFiles} skippedFiles=${summary.skippedFiles}`,
    );
  }
}
