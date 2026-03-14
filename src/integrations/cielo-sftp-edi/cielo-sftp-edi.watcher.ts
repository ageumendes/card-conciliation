import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { promises as fs } from 'fs';
import * as path from 'path';
import * as chokidar from 'chokidar';
import type { FSWatcher } from 'chokidar';
import { CieloSftpEdiService } from './cielo-sftp-edi.service';

@Injectable()
export class CieloSftpEdiWatcher implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(CieloSftpEdiWatcher.name);
  private watcher?: FSWatcher;
  private debounceTimer?: NodeJS.Timeout;
  private isRunning = false;
  private pending = false;

  constructor(
    private readonly configService: ConfigService,
    private readonly ediService: CieloSftpEdiService,
  ) {}

  async onModuleInit(): Promise<void> {
    if (!this.isEnabled()) {
      return;
    }
    await this.startWatching();
  }

  async onModuleDestroy(): Promise<void> {
    if (this.watcher) {
      await this.watcher.close();
      this.watcher = undefined;
    }
  }

  private isEnabled(): boolean {
    const value = this.configService.get<string>('CIELO_EDI_WATCH_ENABLED');
    return (value ?? '').trim().toLowerCase() === 'true';
  }

  private getWatchDir(): string {
    const configured = this.configService.get<string>('CIELO_EDI_WATCH_DIR');
    const fallback = this.ediService.getDirectories().localDir;
    return path.resolve(process.cwd(), configured ?? fallback);
  }

  private getDebounceMs(): number {
    const value = this.configService.get<string>('CIELO_EDI_WATCH_DEBOUNCE_MS');
    const parsed = value ? Number(value) : 1500;
    return Number.isFinite(parsed) ? parsed : 1500;
  }

  private getStableMs(): number {
    const value = this.configService.get<string>('CIELO_EDI_WATCH_STABLE_MS');
    const parsed = value ? Number(value) : 1500;
    return Number.isFinite(parsed) ? parsed : 1500;
  }

  private async startWatching(): Promise<void> {
    const watchDir = this.getWatchDir();
    if (watchDir.startsWith('/sftp/')) {
      this.logger.warn(
        `Watch dir configurado como ${watchDir}. Esse caminho parece ser do server-SFTP; no server-APP-CARD use um caminho local (ex.: ./data/cielo/edi).`,
      );
    }

    try {
      await fs.mkdir(watchDir, { recursive: true });
    } catch (error) {
      const nodeError = error as NodeJS.ErrnoException;
      if (nodeError?.code === 'EACCES' || nodeError?.code === 'EPERM') {
        this.logger.error(
          `Sem permissão para acessar/criar o diretório de watch (${watchDir}). Watcher Cielo não será iniciado.`,
        );
        return;
      }
      throw error;
    }

    this.watcher = chokidar.watch(watchDir, {
      ignoreInitial: true,
      awaitWriteFinish: {
        stabilityThreshold: this.getStableMs(),
        pollInterval: 200,
      },
    });

    this.watcher.on('add', (filePath) => {
      void this.handleFileEvent(filePath, 'add');
    });

    this.watcher.on('change', (filePath) => {
      void this.handleFileEvent(filePath, 'change');
    });

    this.watcher.on('error', (error) => {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`Erro no watcher: ${message}`);
    });

    this.logger.log(`Watcher ativo em ${watchDir}`);
  }

  private async handleFileEvent(filePath: string, event: 'add' | 'change'): Promise<void> {
    const filename = path.basename(filePath);
    if (this.isTempFile(filename)) {
      return;
    }

    this.logger.log(`Novo arquivo detectado (${event}): ${filename}`);

    const isValid = this.isValidFilename(filename);
    const { size } = await fs.stat(filePath);
    if (!isValid || size === 0) {
      await this.moveToError(filePath, isValid ? 'Arquivo vazio' : 'Arquivo invalido');
      return;
    }

    this.scheduleScan();
  }

  private scheduleScan(): void {
    const debounceMs = this.getDebounceMs();
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
    }
    this.debounceTimer = setTimeout(() => {
      void this.runScan();
    }, debounceMs);
  }

  private async runScan(): Promise<void> {
    if (this.isRunning) {
      this.pending = true;
      this.logger.log('Scan coalescido (pending)');
      return;
    }

    this.isRunning = true;
    try {
      this.logger.log('Scan iniciado');
      const result = await this.ediService.scanLocal();
      this.logger.log(
        `Scan finalizado: processados=${result.processed} erros=${result.errors} ignorados=${result.skipped} desconhecidos=${result.unknown}`,
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`Falha no scan: ${message}`);
    } finally {
      this.isRunning = false;
      if (this.pending) {
        this.pending = false;
        this.scheduleScan();
      }
    }
  }

  private isValidFilename(filename: string): boolean {
    return /^CIELO(0(3|4)D|16D)_.*\.TXT$/i.test(filename);
  }

  private isTempFile(filename: string): boolean {
    const lower = filename.toLowerCase();
    return lower.startsWith('~') || lower.endsWith('.part') || lower.endsWith('.tmp');
  }

  private async moveToError(filePath: string, reason: string): Promise<void> {
    const { errorDir } = this.ediService.getDirectories();
    const resolvedErrorDir = path.resolve(process.cwd(), errorDir);
    await fs.mkdir(resolvedErrorDir, { recursive: true });
    await this.moveFile(filePath, resolvedErrorDir);
    this.logger.warn(`Arquivo movido para error (${reason}): ${path.basename(filePath)}`);
  }

  private async moveFile(fromPath: string, targetDir: string): Promise<void> {
    const baseName = path.basename(fromPath);
    let targetPath = path.join(targetDir, baseName);

    try {
      await fs.rename(fromPath, targetPath);
    } catch (error) {
      const timestamp = Date.now();
      targetPath = path.join(targetDir, `${timestamp}-${baseName}`);
      await fs.rename(fromPath, targetPath);
    }
  }
}
