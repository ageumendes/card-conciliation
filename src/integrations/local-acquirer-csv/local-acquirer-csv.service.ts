import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { promises as fs } from 'fs';
import * as path from 'path';
import { AcquirerImportService } from '../acquirer-import/acquirer-import.service';

type AcquirerKind = 'SIPAG' | 'SICREDI';

type AcquirerDirs = {
  watchDir: string;
  archiveDir: string;
  errorDir: string;
};

export type LocalCsvScanSummary = {
  processedFiles: number;
  okFiles: number;
  errorFiles: number;
  skippedFiles: number;
};

@Injectable()
export class LocalAcquirerCsvService {
  private readonly logger = new Logger(LocalAcquirerCsvService.name);

  constructor(
    private readonly configService: ConfigService,
    private readonly acquirerImportService: AcquirerImportService,
  ) {}

  getDebounceMs(): number {
    return this.parsePositiveInt(this.configService.get<string>('CSV_WATCH_DEBOUNCE_MS'), 1500);
  }

  getStableMs(): number {
    return this.parsePositiveInt(this.configService.get<string>('CSV_WATCH_STABLE_MS'), 1500);
  }

  getSipagEnabled(): boolean {
    return this.parseBoolean(this.configService.get<string>('SIPAG_CSV_WATCH_ENABLED'), true);
  }

  getSicrediEnabled(): boolean {
    return this.parseBoolean(this.configService.get<string>('SICREDI_CSV_WATCH_ENABLED'), true);
  }

  getSicrediEdiEnabled(): boolean {
    return this.parseBoolean(this.configService.get<string>('SICREDI_EDI_WATCH_ENABLED'), true);
  }

  getSipagDirs(): AcquirerDirs {
    return {
      watchDir: this.resolveDir(this.configService.get<string>('SIPAG_CSV_WATCH_DIR'), './data/sipag/in'),
      archiveDir: this.resolveDir(
        this.configService.get<string>('SIPAG_CSV_ARCHIVE_DIR'),
        './data/sipag/archive',
      ),
      errorDir: this.resolveDir(this.configService.get<string>('SIPAG_CSV_ERROR_DIR'), './data/sipag/error'),
    };
  }

  getSicrediDirs(): AcquirerDirs {
    return {
      watchDir: this.resolveDir(this.configService.get<string>('SICREDI_CSV_WATCH_DIR'), './data/sicredi/in'),
      archiveDir: this.resolveDir(
        this.configService.get<string>('SICREDI_CSV_ARCHIVE_DIR'),
        './data/sicredi/archive',
      ),
      errorDir: this.resolveDir(
        this.configService.get<string>('SICREDI_CSV_ERROR_DIR'),
        './data/sicredi/error',
      ),
    };
  }

  getSicrediEdiDirs(): AcquirerDirs {
    return {
      watchDir: this.resolveDir(this.configService.get<string>('SICREDI_EDI_WATCH_DIR'), './data/sicredi/edi'),
      archiveDir: this.resolveDir(
        this.configService.get<string>('SICREDI_EDI_ARCHIVE_DIR'),
        './data/sicredi/archive-edi',
      ),
      errorDir: this.resolveDir(
        this.configService.get<string>('SICREDI_EDI_ERROR_DIR'),
        './data/sicredi/error-edi',
      ),
    };
  }

  async scanSipag(): Promise<LocalCsvScanSummary> {
    return this.scanAcquirer('SIPAG');
  }

  async scanSicredi(): Promise<LocalCsvScanSummary> {
    return this.scanAcquirer('SICREDI');
  }

  async scanSicrediEdi(): Promise<LocalCsvScanSummary> {
    const dirs = this.getSicrediEdiDirs();
    await this.ensureDirs(dirs);

    this.logger.log('SICREDI_EDI scan iniciado');

    const summary: LocalCsvScanSummary = {
      processedFiles: 0,
      okFiles: 0,
      errorFiles: 0,
      skippedFiles: 0,
    };

    const entries = await fs.readdir(dirs.watchDir);
    const sortedEntries = [...entries].sort((a, b) => a.localeCompare(b));

    for (const filename of sortedEntries) {
      const fullPath = path.join(dirs.watchDir, filename);
      let stat;
      try {
        stat = await fs.stat(fullPath);
      } catch (error) {
        summary.errorFiles += 1;
        this.logger.error(`SICREDI_EDI erro ao obter stat arquivo=${filename}: ${this.errorMessage(error)}`);
        continue;
      }

      if (!stat.isFile()) {
        summary.skippedFiles += 1;
        continue;
      }

      if (this.isTempFile(filename) || !this.isJsonFile(filename)) {
        summary.skippedFiles += 1;
        continue;
      }

      summary.processedFiles += 1;

      if (stat.size === 0) {
        await this.moveToDir(fullPath, dirs.errorDir);
        summary.errorFiles += 1;
        this.logger.warn(`SICREDI_EDI arquivo vazio movido para error arquivo=${filename}`);
        continue;
      }

      try {
        const buffer = await fs.readFile(fullPath);
        const result = await this.acquirerImportService.importSicrediEdi(buffer, filename);

        await this.moveToDir(fullPath, dirs.archiveDir);
        summary.okFiles += 1;
        this.logger.log(
          `SICREDI_EDI import OK arquivo=${filename} inserted=${result.inserted} duplicates=${result.duplicates} invalidRows=${result.invalidRows}`,
        );
      } catch (error) {
        summary.errorFiles += 1;
        this.logger.error(`SICREDI_EDI import erro arquivo=${filename}: ${this.errorMessage(error)}`);
        await this.safeMoveToError(fullPath, dirs.errorDir, 'SICREDI', filename);
      }
    }

    return summary;
  }

  async ensureAllConfiguredDirs(): Promise<void> {
    if (this.getSipagEnabled()) {
      const dirs = this.getSipagDirs();
      await this.ensureDirs(dirs);
    }

    if (this.getSicrediEnabled()) {
      const dirs = this.getSicrediDirs();
      await this.ensureDirs(dirs);
    }

    if (this.getSicrediEdiEnabled()) {
      const dirs = this.getSicrediEdiDirs();
      await this.ensureDirs(dirs);
    }
  }

  private async scanAcquirer(kind: AcquirerKind): Promise<LocalCsvScanSummary> {
    const dirs = this.getDirs(kind);
    await this.ensureDirs(dirs);

    this.logger.log(`${kind} scan iniciado`);

    const summary: LocalCsvScanSummary = {
      processedFiles: 0,
      okFiles: 0,
      errorFiles: 0,
      skippedFiles: 0,
    };

    const entries = await fs.readdir(dirs.watchDir);
    const sortedEntries = [...entries].sort((a, b) => a.localeCompare(b));

    for (const filename of sortedEntries) {
      const fullPath = path.join(dirs.watchDir, filename);
      let stat;
      try {
        stat = await fs.stat(fullPath);
      } catch (error) {
        summary.errorFiles += 1;
        this.logger.error(`${kind} erro ao obter stat arquivo=${filename}: ${this.errorMessage(error)}`);
        continue;
      }

      if (!stat.isFile()) {
        summary.skippedFiles += 1;
        continue;
      }

      if (this.isTempFile(filename) || !this.isCsvFile(filename)) {
        summary.skippedFiles += 1;
        continue;
      }

      summary.processedFiles += 1;

      if (stat.size === 0) {
        await this.moveToDir(fullPath, dirs.errorDir);
        summary.errorFiles += 1;
        this.logger.warn(`${kind} arquivo vazio movido para error arquivo=${filename}`);
        continue;
      }

      try {
        const buffer = await fs.readFile(fullPath);
        const result =
          kind === 'SIPAG'
            ? await this.acquirerImportService.importSipag(buffer, filename)
            : await this.acquirerImportService.importSicredi(buffer, filename);

        await this.moveToDir(fullPath, dirs.archiveDir);
        summary.okFiles += 1;
        this.logger.log(
          `${kind} import OK arquivo=${filename} inserted=${result.inserted} duplicates=${result.duplicates} invalidRows=${result.invalidRows}`,
        );
      } catch (error) {
        summary.errorFiles += 1;
        this.logger.error(`${kind} import erro arquivo=${filename}: ${this.errorMessage(error)}`);
        await this.safeMoveToError(fullPath, dirs.errorDir, kind, filename);
      }
    }

    return summary;
  }

  private getDirs(kind: AcquirerKind): AcquirerDirs {
    return kind === 'SIPAG' ? this.getSipagDirs() : this.getSicrediDirs();
  }

  private async ensureDirs(dirs: AcquirerDirs): Promise<void> {
    await fs.mkdir(dirs.watchDir, { recursive: true });
    await fs.mkdir(dirs.archiveDir, { recursive: true });
    await fs.mkdir(dirs.errorDir, { recursive: true });
  }

  private isCsvFile(filename: string): boolean {
    return filename.toLowerCase().endsWith('.csv');
  }

  private isJsonFile(filename: string): boolean {
    return filename.toLowerCase().endsWith('.json');
  }

  private isTempFile(filename: string): boolean {
    const lower = filename.toLowerCase();
    return lower.startsWith('~') || lower.endsWith('.part') || lower.endsWith('.tmp');
  }

  private async moveToDir(fromPath: string, targetDir: string): Promise<string> {
    await fs.mkdir(targetDir, { recursive: true });

    const baseName = path.basename(fromPath);
    let targetName = baseName;
    let targetPath = path.join(targetDir, targetName);

    while (await this.pathExists(targetPath)) {
      targetName = `${Date.now()}-${baseName}`;
      targetPath = path.join(targetDir, targetName);
    }

    try {
      await fs.rename(fromPath, targetPath);
    } catch (error) {
      const nodeError = error as NodeJS.ErrnoException;
      if (nodeError?.code !== 'EXDEV') {
        throw error;
      }
      await fs.copyFile(fromPath, targetPath);
      await fs.unlink(fromPath);
    }

    return targetPath;
  }

  private async safeMoveToError(
    fromPath: string,
    errorDir: string,
    kind: AcquirerKind,
    filename: string,
  ): Promise<void> {
    try {
      if (await this.pathExists(fromPath)) {
        await this.moveToDir(fromPath, errorDir);
      }
    } catch (moveError) {
      this.logger.error(
        `${kind} falha ao mover para error arquivo=${filename}: ${this.errorMessage(moveError)}`,
      );
    }
  }

  private async pathExists(targetPath: string): Promise<boolean> {
    try {
      await fs.access(targetPath);
      return true;
    } catch {
      return false;
    }
  }

  private resolveDir(configured: string | undefined, fallback: string): string {
    return path.resolve(process.cwd(), configured ?? fallback);
  }

  private parsePositiveInt(value: string | undefined, fallback: number): number {
    if (!value) {
      return fallback;
    }
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return fallback;
    }
    return Math.floor(parsed);
  }

  private parseBoolean(value: string | undefined, fallback: boolean): boolean {
    if (value == null) {
      return fallback;
    }
    const normalized = value.trim().toLowerCase();
    if (['1', 'true', 'yes', 'y', 'on'].includes(normalized)) {
      return true;
    }
    if (['0', 'false', 'no', 'n', 'off'].includes(normalized)) {
      return false;
    }
    return fallback;
  }

  private errorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
  }
}
