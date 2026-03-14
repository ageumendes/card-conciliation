import {
  BadRequestException,
  Body,
  ConflictException,
  Controller,
  Get,
  Headers,
  Logger,
  Post,
  Query,
  Req,
  Res,
  UploadedFile,
  UseInterceptors,
  UseGuards,
} from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { Request, Response } from 'express';
import { InterdataImportService } from './interdata-import.service';
import { InterdataApproveDto, InterdataListSalesQueryDto } from './interdata-import.dto';
import { InterdataProgressService } from './interdata-progress.service';
import { AdminGuard } from '../../auth/admin.guard';
import { ReconciliationService } from '../../modules/reconciliation/reconciliation.service';
import { parseBoolean } from '../../modules/reconciliation/reconciliation.utils';
import { RemoteEdiService } from '../remote-edi/remote-edi.service';

@Controller('admin/interdata')
@UseGuards(AdminGuard)
export class InterdataImportController {
  private readonly logger = new Logger(InterdataImportController.name);

  constructor(
    private readonly interdataService: InterdataImportService,
    private readonly progressService: InterdataProgressService,
    private readonly reconciliationService: ReconciliationService,
    private readonly remoteEdiService: RemoteEdiService,
  ) {}

  @Get('ping')
  ping() {
    return {
      ok: true,
      enabled: this.interdataService.getEnabled(),
      dirs: this.interdataService.getDirectories(),
      watch: {
        enabled: this.interdataService.getWatchEnabled(),
        debounceMs: this.interdataService.getWatchDebounceMs(),
        stableMs: this.interdataService.getWatchStableMs(),
      },
    };
  }

  @Get('files')
  async listFiles() {
    this.ensureEnabled();
    const files = await this.interdataService.listDropFiles();
    return { ok: true, files };
  }

  @Post('import/scan')
  async scan() {
    this.ensureEnabled();
    this.progressService.startManualImport();
    try {
      const result = await this.interdataService.scanDropDir();
      this.triggerReconciliationInBackground(
        'interdata-scan',
        result.reconciliationDateFrom ?? undefined,
        result.reconciliationDateTo ?? undefined,
      );
      return { ok: true, ...result };
    } finally {
      this.progressService.endManualImport();
    }
  }

  @Post('import/upload')
  @UseInterceptors(FileInterceptor('file'))
  async upload(
    @UploadedFile() file?: Express.Multer.File,
    @Headers('x-upload-id') uploadId?: string,
  ) {
    this.ensureEnabled();
    const effectiveUploadId = uploadId?.trim() || `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    this.logger.log(
      `Upload recebido: ${file ? `${file.originalname} (${file.size} bytes)` : 'sem arquivo'}`,
    );
    if (!file) {
      throw new BadRequestException('Arquivo nao enviado');
    }

    try {
      this.logger.log(`Processando upload: ${file.originalname}`);
      this.progressService.start(effectiveUploadId, file.originalname);
      const result = await this.interdataService.uploadAndImport(
        file.buffer,
        file.originalname,
        effectiveUploadId,
      );
      this.progressService.complete(effectiveUploadId);
      if (!result.alreadyImported) {
        this.triggerReconciliationInBackground(
          'interdata-upload',
          result.reconciliationDateFrom ?? undefined,
          result.reconciliationDateTo ?? undefined,
        );
      }
      this.logger.log(`Upload processado: ${file.originalname}`);
      return { ok: true, uploadId: effectiveUploadId, ...result };
    } catch (error) {
      const err = error instanceof Error ? error : new Error('Erro no upload');
      this.logger.error(`Falha no upload: ${err.message}`, err.stack);
      this.progressService.error(effectiveUploadId, err.message);
      throw err;
    }
  }

  @Get('import/progress')
  progress(@Query('uploadId') uploadId: string, @Req() req: Request, @Res() res: Response) {
    this.ensureEnabled();
    if (!uploadId) {
      throw new BadRequestException('uploadId invalido');
    }

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    const send = (payload: unknown) => {
      res.write(`data: ${JSON.stringify(payload)}\n\n`);
    };

    const current = this.progressService.get(uploadId);
    if (current) {
      send(current);
    }

    const unsubscribe = this.progressService.subscribe(uploadId, (payload) => send(payload));
    const heartbeat = setInterval(() => {
      res.write('event: ping\ndata: {}\n\n');
    }, 10_000);

    req.on('close', () => {
      clearInterval(heartbeat);
      unsubscribe();
      res.end();
    });
  }

  @Get('import/details')
  async details(
    @Query('metric') metric?: string,
    @Query('limit') limit?: string,
  ) {
    this.ensureEnabled();
    const normalizedMetric = metric?.trim().toLowerCase();
    const validMetrics = ['files', 'inserted', 'duplicates', 'invalid', 'review', 'errors'];
    if (!normalizedMetric || !validMetrics.includes(normalizedMetric)) {
      throw new BadRequestException('metric invalido');
    }
    const parsedLimit = limit ? Number(limit) : 20;
    const data = await this.interdataService.listRecentImportDetails(
      normalizedMetric as 'files' | 'inserted' | 'duplicates' | 'invalid' | 'review' | 'errors',
      Number.isFinite(parsedLimit) ? parsedLimit : 20,
    );
    return { ok: true, data };
  }

  @Get('sales')
  async listSales(@Query() query: InterdataListSalesQueryDto) {
    this.ensureEnabled();

    const page = query.page ? Number(query.page) : 1;
    const rawLimit = query.limit ? Number(query.limit) : 50;
    const limit = Math.min(Math.max(rawLimit, 1), 500);
    const normalize = (value?: string) => (value ? value.trim() : undefined);
    const normalizeBucket = (value?: string) => {
      const bucket = value?.trim().toLowerCase();
      if (!bucket) {
        return undefined;
      }
      if (bucket === 'valid' || bucket === 'invalid' || bucket === 'duplicate') {
        return bucket;
      }
      throw new BadRequestException('bucket invalido');
    };

    if (query.dateFrom && !/^\d{4}-\d{2}-\d{2}$/.test(query.dateFrom)) {
      throw new BadRequestException('dateFrom invalido');
    }
    if (query.dateTo && !/^\d{4}-\d{2}-\d{2}$/.test(query.dateTo)) {
      throw new BadRequestException('dateTo invalido');
    }
    const sortBy = query.sortBy?.trim().toLowerCase();
    if (sortBy && sortBy !== 'datetime' && sortBy !== 'amount') {
      throw new BadRequestException('sortBy invalido');
    }
    const sortDir = query.sortDir?.trim().toLowerCase();
    if (sortDir && sortDir !== 'asc' && sortDir !== 'desc') {
      throw new BadRequestException('sortDir invalido');
    }
    const verboseEnabled = parseBoolean(query.verbose) ?? false;

    const data = await this.interdataService.listSales({
      dateFrom: normalize(query.dateFrom),
      dateTo: normalize(query.dateTo),
      page: Number.isNaN(page) || page < 1 ? 1 : page,
      limit,
      status: normalize(query.status),
      acquirer: normalize(query.acquirer),
      search: normalize(query.search),
      paymentType: normalize(query.paymentType),
      brand: normalize(query.brand),
      bucket: normalizeBucket(query.bucket),
      sortBy: sortBy as 'datetime' | 'amount' | undefined,
      sortDir: sortDir as 'asc' | 'desc' | undefined,
      verboseEnabled,
    });
    return { ok: true, data };
  }

  @Post('sales/approve')
  async approve(@Body() body: InterdataApproveDto) {
    this.ensureEnabled();
    const id = body.id ? Number(body.id) : NaN;
    const bucket = body.bucket?.trim().toLowerCase();
    if (!id || Number.isNaN(id)) {
      throw new BadRequestException('id invalido');
    }
    if (bucket !== 'invalid' && bucket !== 'duplicate') {
      throw new BadRequestException('bucket invalido');
    }

    await this.interdataService.approveReviewSale(bucket, id);
    return { ok: true };
  }

  @Post('sales/clear')
  async clearSales() {
    this.ensureEnabled();
    await this.interdataService.clearSales();
    return { ok: true };
  }

  @Post('reconciliation/run')
  async runReconciliation(
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
  ) {
    this.ensureEnabled();
    if (dateFrom && !/^\d{4}-\d{2}-\d{2}$/.test(dateFrom)) {
      throw new BadRequestException('dateFrom invalido');
    }
    if (dateTo && !/^\d{4}-\d{2}-\d{2}$/.test(dateTo)) {
      throw new BadRequestException('dateTo invalido');
    }
    this.logger.log('Clique recebido: botão "Conciliação automática" acionado');
    const result = await this.runReconciliationIfNeeded('manual', dateFrom, dateTo);
    return { ok: true, ...result };
  }

  private ensureEnabled() {
    if (!this.interdataService.getEnabled()) {
      throw new BadRequestException('INTERDATA_ENABLED=false');
    }
  }

  private triggerReconciliationInBackground(source: string, dateFrom?: string, dateTo?: string) {
    void this.runReconciliationIfNeeded(source, dateFrom, dateTo).catch((error) => {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Falha assíncrona na conciliação automática (${source}): ${message}`,
        error instanceof Error ? error.stack : undefined,
      );
    });
  }

  private async runReconciliationIfNeeded(source: string, dateFrom?: string, dateTo?: string) {
    const runId = this.createRunId();
    if (this.progressService.isBusy()) {
      return { ran: false, reason: 'busy' };
    }
    const hasPending = await this.reconciliationService.hasPendingInterdataSales({ dateFrom, dateTo });
    if (!hasPending) {
      return { ran: false, reason: 'empty' };
    }
    const startedAt = Date.now();
    this.logger.log(`Rotina iniciada: conciliação automática (${source}) runId=${runId}`);
    try {
      let preSync = { downloaded: 0, imported: 0, ok: true };
      if (source !== 'manual') {
        preSync = await this.syncRemoteBeforeReconciliation(runId, source, dateFrom, dateTo);
      }
      const result = await this.reconciliationService.reconcile({
        limit: 10000,
        dateFrom,
        dateTo,
      });
      const elapsedMs = Date.now() - startedAt;
      this.logger.log(`Rotina concluída: conciliação automática (${source}) runId=${runId} em ${elapsedMs}ms`);
      this.logExecutionSummary(runId, source, dateFrom, dateTo, 10000, preSync, result);
      return { ran: true, result };
    } catch (error) {
      const elapsedMs = Date.now() - startedAt;
      const message = error instanceof Error ? error.message : String(error);
      if (error instanceof ConflictException && message.includes('reconciliation_already_running')) {
        this.logger.warn(
          `Rotina não iniciada: conciliação automática (${source}) runId=${runId} em ${elapsedMs}ms motivo=reconciliation_already_running`,
        );
        return { ran: false, reason: 'running' };
      }
      this.logger.error(
        `Rotina concluída com erro: conciliação automática (${source}) runId=${runId} em ${elapsedMs}ms motivo=${message}`,
      );
      throw error;
    }
  }

  private async syncRemoteBeforeReconciliation(
    runId: string,
    source: string,
    dateFrom?: string,
    dateTo?: string,
  ): Promise<{ downloaded: number; imported: number; ok: boolean }> {
    const startedAt = Date.now();
    this.logger.log(
      `Rotina iniciada: pre-sync remoto (${source}) runId=${runId} dateFrom=${dateFrom ?? 'N/A'} dateTo=${dateTo ?? 'N/A'}`,
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
        `Rotina concluída: pre-sync remoto (${source}) runId=${runId} em ${elapsedMs}ms baixados=${pull.summary.total.downloaded} importados=${imported.summary.totalImported}`,
      );
      return {
        downloaded: pull.summary.total.downloaded,
        imported: imported.summary.totalImported,
        ok: pull.ok && imported.ok,
      };
    } catch (error) {
      const elapsedMs = Date.now() - startedAt;
      const message = error instanceof Error ? error.message : String(error);
      this.logger.warn(
        `Rotina concluída com aviso: pre-sync remoto (${source}) runId=${runId} em ${elapsedMs}ms motivo=${message}. Conciliação seguirá sem bloquear.`,
      );
      return { downloaded: 0, imported: 0, ok: false };
    }
  }

  private logExecutionSummary(
    runId: string,
    source: string,
    dateFrom: string | undefined,
    dateTo: string | undefined,
    limit: number,
    preSync: { downloaded: number; imported: number; ok: boolean },
    result: any,
  ) {
    const summary = result?.data ?? {};
    this.logger.log(
      `[AUTO-RECON SUMMARY] runId=${runId} source=${source} dateFrom=${dateFrom ?? 'N/A'} dateTo=${dateTo ?? 'N/A'} limit=${limit} remoteOk=${preSync.ok} remoteDownloaded=${preSync.downloaded} remoteImported=${preSync.imported} processed=${summary.processed ?? 0} matched=${summary.matched ?? 0} insertedRecon=${summary.insertedRecon ?? 0} pending=${summary.pending ?? 0} errors=${summary.errors ?? 0}`,
    );
  }

  private createRunId(): string {
    return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
  }
}
