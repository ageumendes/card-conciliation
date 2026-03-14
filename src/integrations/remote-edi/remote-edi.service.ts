import { ConflictException, Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { promises as fs } from 'fs';
import * as path from 'path';
import { CieloSftpEdiService } from '../cielo-sftp-edi/cielo-sftp-edi.service';
import { LocalAcquirerCsvService } from '../local-acquirer-csv/local-acquirer-csv.service';
import { appendImportLog } from '../../common/import-log';

type ProviderKey = 'cielo' | 'sipag' | 'sicredi';
type ProviderLabel = 'CIELO' | 'SIPAG' | 'SICREDI';

type ProviderRunSummary = {
  listed: number;
  skippedRecent: number;
  downloaded: number;
  imported: number;
  movedProcessed: number;
  movedError: number;
  ignored: number;
  errors: string[];
};

type ProviderConfig = {
  key: ProviderKey;
  label: ProviderLabel;
  user: string;
  inDir: string;
  processedDir: string;
  errorDir: string;
  localInDir: string;
  localArchiveDir: string;
  localMirrorDir: string;
  localUnknownDir: string;
  allowlist: RegExp;
};

type RemoteFileCandidate = {
  name: string;
  remotePath: string;
  localPath: string;
  mirrorPath?: string;
};

export type RemoteEdiPullOptions = {
  cielo?: boolean;
  sipag?: boolean;
  sicredi?: boolean;
  dryRun?: boolean;
  moveUnknownToError?: boolean;
};

export type RemoteEdiImportOptions = {
  cielo?: boolean;
  sipag?: boolean;
  sicredi?: boolean;
};

type ProviderSelectionOptions = {
  cielo: boolean;
  sipag: boolean;
  sicredi: boolean;
};

type RemoteEdiResult = {
  ok: boolean;
  host: string;
  startedAt: string;
  finishedAt: string;
  options: Required<RemoteEdiPullOptions>;
  summary: {
    cielo: ProviderRunSummary;
    sipag: ProviderRunSummary;
    sicredi: ProviderRunSummary;
    total: {
      listed: number;
      downloaded: number;
      movedProcessed: number;
      movedError: number;
      ignored: number;
    };
  };
};

type RemoteEdiImportResult = {
  ok: boolean;
  startedAt: string;
  finishedAt: string;
  options: ProviderSelectionOptions;
  summary: {
    cielo: { imported: number; errors: string[] };
    sipag: { imported: number; errors: string[] };
    sicredi: { imported: number; errors: string[] };
    totalImported: number;
  };
};

@Injectable()
export class RemoteEdiService {
  private readonly logger = new Logger(RemoteEdiService.name);
  private isRunning = false;

  constructor(
    private readonly configService: ConfigService,
    private readonly cieloEdiService: CieloSftpEdiService,
    private readonly localAcquirerCsvService: LocalAcquirerCsvService,
  ) {}

  getPingInfo() {
    const providers = this.buildProviders();
    return {
      enabled: this.getEnabled(),
      host: this.getHost(),
      port: this.getPort(),
      stableSeconds: this.getStableSeconds(),
      maxFilesPerRun: this.getMaxFilesPerRun(),
      moveUnknownToErrorDefault: this.getMoveUnknownToErrorDefault(),
      users: {
        cielo: providers.cielo.user,
        sipag: providers.sipag.user,
        sicredi: providers.sicredi.user,
      },
      remoteDirs: {
        cielo: {
          inDir: providers.cielo.inDir,
          processedDir: providers.cielo.processedDir,
          errorDir: providers.cielo.errorDir,
        },
        sipag: {
          inDir: providers.sipag.inDir,
          processedDir: providers.sipag.processedDir,
          errorDir: providers.sipag.errorDir,
        },
        sicredi: {
          inDir: providers.sicredi.inDir,
          processedDir: providers.sicredi.processedDir,
          errorDir: providers.sicredi.errorDir,
        },
      },
        localDirs: {
          baseDir: this.getLocalBaseDir(),
          cielo: {
            inDir: providers.cielo.localInDir,
            archiveDir: providers.cielo.localArchiveDir,
            mirrorDir: providers.cielo.localMirrorDir,
            unknownDir: providers.cielo.localUnknownDir,
          },
          sipag: {
            inDir: providers.sipag.localInDir,
            archiveDir: providers.sipag.localArchiveDir,
            mirrorDir: providers.sipag.localMirrorDir,
            unknownDir: providers.sipag.localUnknownDir,
          },
          sicredi: {
            inDir: providers.sicredi.localInDir,
            archiveDir: providers.sicredi.localArchiveDir,
            mirrorDir: providers.sicredi.localMirrorDir,
            unknownDir: providers.sicredi.localUnknownDir,
          },
        },
      allowlistRegex: {
        cielo: providers.cielo.allowlist.source,
        sipag: providers.sipag.allowlist.source,
        sicredi: providers.sicredi.allowlist.source,
      },
    };
  }

  async pull(options: RemoteEdiPullOptions = {}): Promise<RemoteEdiResult> {
    if (this.isRunning) {
      throw new ConflictException('Remote pull já em execução');
    }

    this.isRunning = true;
    const startedAt = new Date().toISOString();
    const resolvedOptions = this.resolveOptions(options);

    const result: RemoteEdiResult = {
      ok: true,
      host: this.getHost(),
      startedAt,
      finishedAt: startedAt,
      options: resolvedOptions,
      summary: {
        cielo: this.emptyProviderSummary(),
        sipag: this.emptyProviderSummary(),
        sicredi: this.emptyProviderSummary(),
        total: {
          listed: 0,
          downloaded: 0,
          movedProcessed: 0,
          movedError: 0,
          ignored: 0,
        },
      },
    };

    try {
      if (!this.getEnabled()) {
        throw new Error('REMOTE_EDI_ENABLED=false');
      }

      const providersMap = this.buildProviders();
      const selectedProviders = this.selectedProviders(resolvedOptions, providersMap);
      if (selectedProviders.length === 0) {
        result.finishedAt = new Date().toISOString();
        return result;
      }

      await this.ensureLocalDirs(providersMap);
      const privateKey = await this.loadPrivateKey();

      const maxFilesPerRun = this.getMaxFilesPerRun();
      let remainingQuota = maxFilesPerRun;

      for (const provider of selectedProviders) {
        const summary = result.summary[provider.key];

        if (remainingQuota <= 0) {
          this.logger.log(`[RemoteEdi] limite global atingido; provider=${provider.label} sem seleção de arquivos`);
          this.logProviderFinal(provider.label, summary);
          continue;
        }

        const sftp = this.createSftpClient();
        try {
          await this.connectFor(sftp, provider, privateKey);
          await this.ensureRemoteDir(sftp, provider.processedDir, provider.label, 'processed');
          await this.ensureRemoteDir(sftp, provider.errorDir, provider.label, 'error');

          const listed = await this.listCandidates(sftp, provider, summary, resolvedOptions.moveUnknownToError, resolvedOptions.dryRun);
          summary.listed = listed.length;

          const selected = listed.slice(0, remainingQuota);
          remainingQuota = Math.max(0, remainingQuota - selected.length);

          this.logger.log(
            `[RemoteEdi] ${provider.label} encontrados=${listed.length} selecionados=${selected.length} skippedRecent=${summary.skippedRecent}`,
          );

          if (!resolvedOptions.dryRun && selected.length > 0) {
            const downloaded = await this.downloadProviderFiles(sftp, provider, selected, summary);

            for (const file of downloaded) {
              try {
                await this.remoteMoveWithCollision(sftp, file.remotePath, provider.processedDir);
                summary.movedProcessed += 1;
                this.logger.log(`[RemoteEdi] ${provider.label} movido remoto ${file.name} -> processed`);
              } catch (error) {
                const message = this.errorMessage(error);
                summary.errors.push(`Falha ao mover remoto ${file.name}: ${message}`);
                this.logger.error(`[RemoteEdi] ${provider.label} falha ao mover remoto ${file.name}: ${message}`);
              }
            }
          }

          this.logProviderFinal(provider.label, summary);
          await appendImportLog({
            category: 'remote_pull',
            provider: provider.label,
            operation: 'pull_provider',
            status: summary.errors.length ? 'error' : 'ok',
            details: {
              listed: summary.listed,
              downloaded: summary.downloaded,
              movedProcessed: summary.movedProcessed,
              movedError: summary.movedError,
              ignored: summary.ignored,
              errors: summary.errors,
            },
          });
        } catch (error) {
          const message = this.errorMessage(error);
          summary.errors.push(`Falha geral provider ${provider.label}: ${message}`);
          this.logger.error(`[RemoteEdi] falha provider=${provider.label}: ${message}`);
          this.logProviderFinal(provider.label, summary);
          await appendImportLog({
            category: 'remote_pull',
            provider: provider.label,
            operation: 'pull_provider',
            status: 'error',
            details: { error: message, errors: summary.errors },
          });
        } finally {
          try {
            await sftp.end();
          } catch {
            // ignore disconnect errors
          }
        }
      }

      result.summary.total = this.computeTotal(result.summary);
      result.ok = this.hasNoErrors(result.summary);
      result.finishedAt = new Date().toISOString();
      await appendImportLog({
        category: 'remote_pull',
        provider: 'ALL',
        operation: 'pull_total',
        status: result.ok ? 'ok' : 'error',
        details: result.summary.total as unknown as Record<string, unknown>,
      });
      return result;
    } finally {
      this.isRunning = false;
    }
  }

  async importLocal(options: RemoteEdiImportOptions = {}): Promise<RemoteEdiImportResult> {
    if (this.isRunning) {
      throw new ConflictException('Remote pull já em execução');
    }

    this.isRunning = true;
    const startedAt = new Date().toISOString();
    const resolvedOptions = this.resolveProviderOptions(options);

    const result: RemoteEdiImportResult = {
      ok: true,
      startedAt,
      finishedAt: startedAt,
      options: resolvedOptions,
      summary: {
        cielo: { imported: 0, errors: [] },
        sipag: { imported: 0, errors: [] },
        sicredi: { imported: 0, errors: [] },
        totalImported: 0,
      },
    };

    try {
      if (resolvedOptions.cielo) {
        try {
          const scan = await this.cieloEdiService.scanLocal();
          result.summary.cielo.imported = scan.processed;
          await appendImportLog({
            category: 'remote_import',
            provider: 'CIELO',
            operation: 'import_local_scan',
            status: 'ok',
            details: scan as unknown as Record<string, unknown>,
          });
        } catch (error) {
          const message = this.errorMessage(error);
          result.summary.cielo.errors.push(message);
          this.logger.error(`[RemoteEdi] import local CIELO ERRO: ${message}`);
          await appendImportLog({
            category: 'remote_import',
            provider: 'CIELO',
            operation: 'import_local_scan',
            status: 'error',
            details: { error: message },
          });
        }
      }

      if (resolvedOptions.sipag) {
        try {
          const scan = await this.localAcquirerCsvService.scanSipag();
          result.summary.sipag.imported = scan.okFiles;
          await appendImportLog({
            category: 'remote_import',
            provider: 'SIPAG',
            operation: 'import_local_scan',
            status: 'ok',
            details: scan as unknown as Record<string, unknown>,
          });
        } catch (error) {
          const message = this.errorMessage(error);
          result.summary.sipag.errors.push(message);
          this.logger.error(`[RemoteEdi] import local SIPAG ERRO: ${message}`);
          await appendImportLog({
            category: 'remote_import',
            provider: 'SIPAG',
            operation: 'import_local_scan',
            status: 'error',
            details: { error: message },
          });
        }
      }

      if (resolvedOptions.sicredi) {
        try {
          const scan = await this.localAcquirerCsvService.scanSicrediEdi();
          result.summary.sicredi.imported = scan.okFiles;
          await appendImportLog({
            category: 'remote_import',
            provider: 'SICREDI',
            operation: 'import_local_scan',
            status: 'ok',
            details: scan as unknown as Record<string, unknown>,
          });
        } catch (error) {
          const message = this.errorMessage(error);
          result.summary.sicredi.errors.push(message);
          this.logger.error(`[RemoteEdi] import local SICREDI ERRO: ${message}`);
          await appendImportLog({
            category: 'remote_import',
            provider: 'SICREDI',
            operation: 'import_local_scan',
            status: 'error',
            details: { error: message },
          });
        }
      }

      result.summary.totalImported =
        result.summary.cielo.imported + result.summary.sipag.imported + result.summary.sicredi.imported;
      result.ok =
        result.summary.cielo.errors.length === 0 &&
        result.summary.sipag.errors.length === 0 &&
        result.summary.sicredi.errors.length === 0;
      result.finishedAt = new Date().toISOString();
      await appendImportLog({
        category: 'remote_import',
        provider: 'ALL',
        operation: 'import_local_total',
        status: result.ok ? 'ok' : 'error',
        details: { totalImported: result.summary.totalImported },
      });
      return result;
    } finally {
      this.isRunning = false;
    }
  }

  private createSftpClient(): any {
    const mod = require('ssh2-sftp-client');
    const SftpCtor = mod.default ?? mod;
    return new SftpCtor();
  }

  private async connectFor(sftp: any, provider: ProviderConfig, privateKey: string): Promise<void> {
    this.logger.log(
      `[RemoteEdi] conectando provider=${provider.label} host=${this.getHost()}:${this.getPort()} user=${provider.user}`,
    );

    await sftp.connect({
      host: this.getHost(),
      port: this.getPort(),
      username: provider.user,
      privateKey,
    });
  }

  private async ensureLocalDirs(providers: Record<ProviderKey, ProviderConfig>): Promise<void> {
    await fs.mkdir(this.getLocalBaseDir(), { recursive: true });
    await fs.mkdir(providers.cielo.localInDir, { recursive: true });
    await fs.mkdir(providers.cielo.localArchiveDir, { recursive: true });
    await fs.mkdir(providers.cielo.localMirrorDir, { recursive: true });
    await fs.mkdir(providers.cielo.localUnknownDir, { recursive: true });
    await fs.mkdir(providers.sipag.localInDir, { recursive: true });
    await fs.mkdir(providers.sipag.localArchiveDir, { recursive: true });
    await fs.mkdir(providers.sipag.localMirrorDir, { recursive: true });
    await fs.mkdir(providers.sipag.localUnknownDir, { recursive: true });
    await fs.mkdir(providers.sicredi.localInDir, { recursive: true });
    await fs.mkdir(providers.sicredi.localArchiveDir, { recursive: true });
    await fs.mkdir(providers.sicredi.localMirrorDir, { recursive: true });
    await fs.mkdir(providers.sicredi.localUnknownDir, { recursive: true });
  }

  private async listCandidates(
    sftp: any,
    provider: ProviderConfig,
    summary: ProviderRunSummary,
    moveUnknownToError: boolean,
    dryRun: boolean,
  ): Promise<RemoteFileCandidate[]> {
    const items = await sftp.list(provider.inDir);
    const stableMs = this.getStableSeconds() * 1000;
    const nowMs = Date.now();
    const candidates: RemoteFileCandidate[] = [];

    for (const item of items) {
      if (item.type !== '-') {
        continue;
      }

      const name = item.name;
      if (this.isTempFile(name)) {
        continue;
      }

      if (!provider.allowlist.test(name)) {
        summary.ignored += 1;
        await appendImportLog({
          category: 'remote_pull',
          provider: provider.label,
          operation: 'pull_file',
          status: 'skipped',
          fileName: name,
          details: { reason: 'allowlist_rejected' },
        });
        if (!dryRun) {
          const remoteFrom = path.posix.join(provider.inDir, name);
          await this.downloadLocalSnapshot(sftp, remoteFrom, provider.localUnknownDir, provider.label, 'unknown');
        }
        if (moveUnknownToError && !dryRun) {
          const remoteFrom = path.posix.join(provider.inDir, name);
          try {
            await this.remoteMoveWithCollision(sftp, remoteFrom, provider.errorDir);
            summary.movedError += 1;
            this.logger.log(`[RemoteEdi] ${provider.label} desconhecido movido para error arquivo=${name}`);
          } catch (error) {
            const message = this.errorMessage(error);
            summary.errors.push(`Falha ao mover desconhecido ${name} para error: ${message}`);
            this.logger.error(`[RemoteEdi] ${provider.label} falha mover desconhecido ${name}: ${message}`);
          }
        }
        continue;
      }

      const mtimeMs = this.resolveRemoteMtimeMs(item);
      if (mtimeMs > 0 && nowMs - mtimeMs < stableMs) {
        summary.skippedRecent += 1;
        continue;
      }

      candidates.push({
        name,
        remotePath: path.posix.join(provider.inDir, name),
        localPath: path.join(provider.localInDir, name),
      });
    }

    candidates.sort((a, b) => a.name.localeCompare(b.name));
    return candidates;
  }

  private async downloadProviderFiles(
    sftp: any,
    provider: ProviderConfig,
    files: RemoteFileCandidate[],
    summary: ProviderRunSummary,
  ): Promise<RemoteFileCandidate[]> {
    const downloaded: RemoteFileCandidate[] = [];

    for (const file of files) {
      try {
        if (await this.existsLocalArchiveFile(provider.localArchiveDir, file.name)) {
          await this.remoteMoveWithCollision(sftp, file.remotePath, provider.processedDir);
          summary.movedProcessed += 1;
          await appendImportLog({
            category: 'remote_pull',
            provider: provider.label,
            operation: 'pull_file',
            status: 'skipped',
            fileName: file.name,
            details: { reason: 'already_archived_local' },
          });
          this.logger.log(
            `[RemoteEdi] ${provider.label} arquivo já arquivado localmente (${file.name}); movido remoto para processed`,
          );
          continue;
        }

        const mirrorPath = await this.buildCollisionFreePath(provider.localMirrorDir, file.name);
        const tempLocalPath = `${file.localPath}.part`;
        await sftp.fastGet(file.remotePath, mirrorPath);
        await fs.copyFile(mirrorPath, tempLocalPath);
        await fs.rename(tempLocalPath, file.localPath);
        file.mirrorPath = mirrorPath;
        summary.downloaded += 1;
        downloaded.push(file);
        await appendImportLog({
          category: 'remote_pull',
          provider: provider.label,
          operation: 'pull_file',
          status: 'ok',
          fileName: file.name,
          details: { localPath: file.localPath, mirrorPath },
        });
      } catch (error) {
        const message = this.errorMessage(error);
        summary.errors.push(`Falha no download ${file.name}: ${message}`);
        this.logger.error(`[RemoteEdi] ${provider.label} erro no download arquivo=${file.name}: ${message}`);
        try {
          await fs.unlink(`${file.localPath}.part`);
        } catch {
          // ignore temp cleanup errors
        }
        await appendImportLog({
          category: 'remote_pull',
          provider: provider.label,
          operation: 'pull_file',
          status: 'error',
          fileName: file.name,
          details: { error: message },
        });
      }
    }

    return downloaded;
  }

  private async existsLocalArchiveFile(archiveDir: string, filename: string): Promise<boolean> {
    try {
      await fs.access(path.join(archiveDir, filename));
      return true;
    } catch {
      return false;
    }
  }

  private async remoteMoveWithCollision(
    sftp: any,
    remoteFrom: string,
    remoteToDir: string,
  ): Promise<string> {
    const baseName = path.posix.basename(remoteFrom);
    let targetName = baseName;
    let targetPath = path.posix.join(remoteToDir, targetName);

    let suffix = 0;
    while (await this.remotePathExists(sftp, targetPath)) {
      const prefix = this.timestampPrefix();
      targetName = suffix === 0 ? `${prefix}__${baseName}` : `${prefix}_${suffix}__${baseName}`;
      targetPath = path.posix.join(remoteToDir, targetName);
      suffix += 1;
    }

    await sftp.rename(remoteFrom, targetPath);
    return targetPath;
  }

  private async ensureRemoteDir(
    sftp: any,
    remoteDir: string,
    providerLabel: ProviderLabel,
    kind: 'processed' | 'error',
  ): Promise<void> {
    try {
      const exists = await sftp.exists(remoteDir);
      if (!exists) {
        await sftp.mkdir(remoteDir, true as any);
      }
    } catch (error) {
      const message = this.errorMessage(error);
      this.logger.warn(`[RemoteEdi] warning mkdir remoto ${providerLabel}/${kind}: ${message}`);
    }
  }

  private async remotePathExists(sftp: any, remotePath: string): Promise<boolean> {
    try {
      return Boolean(await sftp.exists(remotePath));
    } catch {
      return false;
    }
  }

  private resolveRemoteMtimeMs(item: any): number {
    const candidate = item.modifyTime ?? item.modifyTimeMs ?? item.mtime ?? item.date;
    if (typeof candidate !== 'number' || !Number.isFinite(candidate)) {
      return 0;
    }

    if (candidate > 1_000_000_000_000) {
      return candidate;
    }
    return candidate * 1000;
  }

  private selectedProviders(
    options: ProviderSelectionOptions,
    providersMap: Record<ProviderKey, ProviderConfig>,
  ): ProviderConfig[] {
    const providers: ProviderConfig[] = [];

    if (options.cielo) {
      providers.push(providersMap.cielo);
    }
    if (options.sipag) {
      providers.push(providersMap.sipag);
    }
    if (options.sicredi) {
      providers.push(providersMap.sicredi);
    }

    return providers;
  }

  private buildProviders(): Record<ProviderKey, ProviderConfig> {
    const cieloDirs = this.cieloEdiService.getDirectories();
    const sipagDirs = this.localAcquirerCsvService.getSipagDirs();
    const sicrediDirs = this.localAcquirerCsvService.getSicrediEdiDirs();

    return {
      cielo: {
        key: 'cielo',
        label: 'CIELO',
        user: this.configService.get<string>('REMOTE_EDI_CIELO_USER') ?? 'cielo_sftp',
        inDir: this.configService.get<string>('REMOTE_EDI_CIELO_IN_DIR') ?? '/in',
        processedDir: this.configService.get<string>('REMOTE_EDI_CIELO_PROCESSED_DIR') ?? '/processed',
        errorDir: this.configService.get<string>('REMOTE_EDI_CIELO_ERROR_DIR') ?? '/error',
        localInDir: path.resolve(process.cwd(), cieloDirs.localDir),
        localArchiveDir: path.resolve(process.cwd(), cieloDirs.archiveDir),
        localMirrorDir: path.join(this.getLocalBaseDir(), 'cielo', 'pulled'),
        localUnknownDir: path.join(this.getLocalBaseDir(), 'cielo', 'unknown'),
        allowlist: this.getAllowlistRegex('cielo'),
      },
      sipag: {
        key: 'sipag',
        label: 'SIPAG',
        user: this.configService.get<string>('REMOTE_EDI_SIPAG_USER') ?? 'sipag_sftp',
        inDir: this.configService.get<string>('REMOTE_EDI_SIPAG_IN_DIR') ?? '/in',
        processedDir: this.configService.get<string>('REMOTE_EDI_SIPAG_PROCESSED_DIR') ?? '/processed',
        errorDir: this.configService.get<string>('REMOTE_EDI_SIPAG_ERROR_DIR') ?? '/error',
        localInDir: sipagDirs.watchDir,
        localArchiveDir: sipagDirs.archiveDir,
        localMirrorDir: path.join(this.getLocalBaseDir(), 'sipag', 'pulled'),
        localUnknownDir: path.join(this.getLocalBaseDir(), 'sipag', 'unknown'),
        allowlist: this.getAllowlistRegex('sipag'),
      },
      sicredi: {
        key: 'sicredi',
        label: 'SICREDI',
        user: this.configService.get<string>('REMOTE_EDI_SICREDI_USER') ?? 'sicredi_sftp',
        inDir: this.configService.get<string>('REMOTE_EDI_SICREDI_IN_DIR') ?? '/in',
        processedDir: this.configService.get<string>('REMOTE_EDI_SICREDI_PROCESSED_DIR') ?? '/processed',
        errorDir: this.configService.get<string>('REMOTE_EDI_SICREDI_ERROR_DIR') ?? '/error',
        localInDir: sicrediDirs.watchDir,
        localArchiveDir: sicrediDirs.archiveDir,
        localMirrorDir: path.join(this.getLocalBaseDir(), 'sicredi', 'pulled'),
        localUnknownDir: path.join(this.getLocalBaseDir(), 'sicredi', 'unknown'),
        allowlist: this.getAllowlistRegex('sicredi'),
      },
    };
  }

  private getAllowlistRegex(provider: ProviderKey): RegExp {
    let configured = '';
    let fallback = '';

    if (provider === 'cielo') {
      configured = this.configService.get<string>('REMOTE_EDI_CIELO_ALLOWLIST_REGEX') ?? '';
      fallback = '^(CIELO(03D|04D|16D)_.*\\.TXT)$';
    } else if (provider === 'sipag') {
      configured = this.configService.get<string>('REMOTE_EDI_SIPAG_ALLOWLIST_REGEX') ?? '';
      fallback = '^.*\\.csv$';
    } else {
      configured = this.configService.get<string>('REMOTE_EDI_SICREDI_ALLOWLIST_REGEX') ?? '';
      fallback = '^.*\\.json$';
    }

    const source = this.normalizeRegexSource(configured.trim() || fallback);
    try {
      return new RegExp(source, 'i');
    } catch (error) {
      const message = this.errorMessage(error);
      this.logger.warn(`[RemoteEdi] regex inválida para ${provider.toUpperCase()} (${source}). Usando fallback. Erro: ${message}`);
      return new RegExp(fallback, 'i');
    }
  }

  private getEnabled(): boolean {
    return this.parseBoolean(this.configService.get<string>('REMOTE_EDI_ENABLED'), true);
  }

  private getHost(): string {
    return this.configService.get<string>('REMOTE_EDI_HOST') ?? '44.220.84.227';
  }

  private getPort(): number {
    return this.parsePositiveInt(this.configService.get<string>('REMOTE_EDI_PORT'), 22);
  }

  private getLocalBaseDir(): string {
    const configured = this.configService.get<string>('REMOTE_EDI_LOCAL_BASE_DIR') ?? './data/remote-edi';
    return path.resolve(process.cwd(), configured);
  }

  private getStableSeconds(): number {
    return this.parsePositiveInt(this.configService.get<string>('REMOTE_EDI_STABLE_SECONDS'), 10);
  }

  private getMaxFilesPerRun(): number {
    return this.parsePositiveInt(this.configService.get<string>('REMOTE_EDI_MAX_FILES_PER_RUN'), 50);
  }

  private getMoveUnknownToErrorDefault(): boolean {
    return this.parseBoolean(this.configService.get<string>('REMOTE_EDI_MOVE_UNKNOWN_TO_ERROR'), false);
  }

  private resolveOptions(options: RemoteEdiPullOptions): Required<RemoteEdiPullOptions> {
    const providers = this.resolveProviderOptions(options);
    return {
      ...providers,
      dryRun: options.dryRun ?? false,
      moveUnknownToError: options.moveUnknownToError ?? this.getMoveUnknownToErrorDefault(),
    };
  }

  private resolveProviderOptions(
    options: RemoteEdiPullOptions | RemoteEdiImportOptions,
  ): ProviderSelectionOptions {
    return {
      cielo: options.cielo ?? true,
      sipag: options.sipag ?? true,
      sicredi: options.sicredi ?? true,
    };
  }

  private emptyProviderSummary(): ProviderRunSummary {
    return {
      listed: 0,
      skippedRecent: 0,
      downloaded: 0,
      imported: 0,
      movedProcessed: 0,
      movedError: 0,
      ignored: 0,
      errors: [],
    };
  }

  private computeTotal(summary: RemoteEdiResult['summary']): RemoteEdiResult['summary']['total'] {
    return {
      listed: summary.cielo.listed + summary.sipag.listed + summary.sicredi.listed,
      downloaded: summary.cielo.downloaded + summary.sipag.downloaded + summary.sicredi.downloaded,
      movedProcessed:
        summary.cielo.movedProcessed + summary.sipag.movedProcessed + summary.sicredi.movedProcessed,
      movedError: summary.cielo.movedError + summary.sipag.movedError + summary.sicredi.movedError,
      ignored: summary.cielo.ignored + summary.sipag.ignored + summary.sicredi.ignored,
    };
  }

  private hasNoErrors(summary: RemoteEdiResult['summary']): boolean {
    return (
      summary.cielo.errors.length === 0 &&
      summary.sipag.errors.length === 0 &&
      summary.sicredi.errors.length === 0
    );
  }

  private async loadPrivateKey(): Promise<string> {
    const privateKeyPath = this.configService.get<string>('REMOTE_EDI_PRIVATE_KEY_PATH');
    const inlinePrivateKey = this.configService.get<string>('REMOTE_EDI_PRIVATE_KEY');

    if (privateKeyPath) {
      return fs.readFile(path.resolve(process.cwd(), privateKeyPath), 'utf8');
    }

    if (inlinePrivateKey) {
      return inlinePrivateKey;
    }

    throw new Error('REMOTE_EDI_PRIVATE_KEY_PATH ou REMOTE_EDI_PRIVATE_KEY deve ser informado');
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

  private isTempFile(filename: string): boolean {
    const lower = filename.toLowerCase();
    return lower.startsWith('~') || lower.endsWith('.tmp') || lower.endsWith('.part');
  }

  private timestampPrefix(): string {
    const now = new Date();
    const pad = (value: number) => value.toString().padStart(2, '0');
    return [
      now.getFullYear().toString(),
      pad(now.getMonth() + 1),
      pad(now.getDate()),
      '_',
      pad(now.getHours()),
      pad(now.getMinutes()),
      pad(now.getSeconds()),
    ].join('');
  }

  private logProviderFinal(label: ProviderLabel, summary: ProviderRunSummary): void {
    this.logger.log(
      `[RemoteEdi] ${label} baixados ${summary.downloaded}; importados ${summary.imported}; movidos para processed ${summary.movedProcessed}; movidos para error ${summary.movedError}; ignorados ${summary.ignored} (por padrão)`,
    );
  }

  private normalizeRegexSource(value: string): string {
    if (!value.includes('\\\\')) {
      return value;
    }
    const normalized = value.replace(/\\\\/g, '\\');
    this.logger.warn(
      `[RemoteEdi] regex allowlist com barras duplas detectada; normalizando "${value}" -> "${normalized}"`,
    );
    return normalized;
  }

  private async downloadLocalSnapshot(
    sftp: any,
    remotePath: string,
    targetDir: string,
    providerLabel: ProviderLabel,
    kind: 'unknown',
  ): Promise<void> {
    try {
      const baseName = path.posix.basename(remotePath);
      const localPath = await this.buildCollisionFreePath(targetDir, baseName);
      await sftp.fastGet(remotePath, localPath);
      this.logger.log(`[RemoteEdi] ${providerLabel} snapshot local (${kind}) salvo arquivo=${baseName}`);
    } catch (error) {
      const message = this.errorMessage(error);
      this.logger.warn(`[RemoteEdi] ${providerLabel} falha snapshot local (${kind}): ${message}`);
    }
  }

  private async buildCollisionFreePath(targetDir: string, baseName: string): Promise<string> {
    let targetPath = path.join(targetDir, baseName);
    let suffix = 0;
    while (await this.pathExists(targetPath)) {
      const prefix = this.timestampPrefix();
      const candidate =
        suffix === 0 ? `${prefix}__${baseName}` : `${prefix}_${suffix}__${baseName}`;
      targetPath = path.join(targetDir, candidate);
      suffix += 1;
    }
    return targetPath;
  }

  private async pathExists(filePath: string): Promise<boolean> {
    try {
      await fs.access(filePath);
      return true;
    } catch {
      return false;
    }
  }

  private errorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
  }
}
