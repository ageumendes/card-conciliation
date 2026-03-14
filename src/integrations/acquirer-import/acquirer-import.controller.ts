import {
  BadRequestException,
  Controller,
  Get,
  Logger,
  Post,
  Query,
  UploadedFile,
  UseInterceptors,
  UseGuards,
} from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { AcquirerImportService } from './acquirer-import.service';
import { AdminGuard } from '../../auth/admin.guard';

@Controller('admin/acquirer-import')
@UseGuards(AdminGuard)
export class AcquirerImportController {
  private readonly logger = new Logger(AcquirerImportController.name);

  constructor(private readonly acquirerImportService: AcquirerImportService) {}

  @Get('ping')
  ping() {
    return { ok: true, acquirers: ['CIELO', 'SIPAG', 'SICREDI'] };
  }

  @Post('upload')
  @UseInterceptors(FileInterceptor('file'))
  async upload(
    @UploadedFile() file?: Express.Multer.File,
    @Query('acquirer') acquirer?: string,
    @Query('format') format?: string,
  ) {
    if (!file) {
      throw new BadRequestException('Arquivo nao enviado');
    }
    const normalized = acquirer?.trim().toLowerCase();
    if (!normalized || (normalized !== 'cielo' && normalized !== 'sipag' && normalized !== 'sicredi')) {
      throw new BadRequestException('acquirer invalido');
    }

    this.logger.log(`Upload recebido: ${file.originalname} (${file.size} bytes)`);
    const normalizedFormat = format?.trim().toLowerCase();
    const isSicrediEdi =
      normalized === 'sicredi' &&
      (normalizedFormat === 'edi' || file.originalname.trim().toLowerCase().endsWith('.json'));

    const result =
      normalized === 'cielo'
        ? await this.acquirerImportService.importCielo(file.buffer, file.originalname)
        : normalized === 'sipag'
          ? await this.acquirerImportService.importSipag(file.buffer, file.originalname)
          : isSicrediEdi
            ? await this.acquirerImportService.importSicrediEdi(file.buffer, file.originalname)
            : await this.acquirerImportService.importSicredi(file.buffer, file.originalname);
    return {
      ok: true,
      acquirer: normalized.toUpperCase(),
      format: isSicrediEdi ? 'EDI' : 'CSV',
      ...result,
    };
  }

  @Get('sales')
  async listSales(
    @Query('acquirer') acquirer?: string,
    @Query('acquirers') acquirers?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('status') status?: string,
    @Query('brand') brand?: string,
    @Query('search') search?: string,
    @Query('paymentType') paymentType?: string,
    @Query('sortBy') sortBy?: string,
    @Query('sortDir') sortDir?: string,
    @Query('includeReconciled') includeReconciled?: string,
  ) {
    const normalized = acquirer?.trim().toLowerCase();
    const acquirerList = acquirers
      ? acquirers
          .split(',')
          .map((value) => value.trim().toLowerCase())
          .filter((value) => value)
      : [];
    const validList = acquirerList.every(
      (value) => value === 'cielo' || value === 'sipag' || value === 'sicredi',
    );
    if (acquirerList.length && !validList) {
      throw new BadRequestException('acquirers invalido');
    }
    if (
      !acquirerList.length &&
      (!normalized || (normalized !== 'cielo' && normalized !== 'sipag' && normalized !== 'sicredi'))
    ) {
      throw new BadRequestException('acquirer invalido');
    }
    if (dateFrom && !/^\d{4}-\d{2}-\d{2}$/.test(dateFrom)) {
      throw new BadRequestException('dateFrom invalido');
    }
    if (dateTo && !/^\d{4}-\d{2}-\d{2}$/.test(dateTo)) {
      throw new BadRequestException('dateTo invalido');
    }
    if (dateFrom && dateTo) {
      const from = new Date(`${dateFrom}T00:00:00.000Z`);
      const to = new Date(`${dateTo}T00:00:00.000Z`);
      if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime())) {
        throw new BadRequestException('intervalo de datas invalido');
      }
      if (to < from) {
        throw new BadRequestException('dateTo deve ser maior ou igual a dateFrom');
      }
      const maxTo = new Date(from);
      maxTo.setUTCMonth(maxTo.getUTCMonth() + 3);
      if (to > maxTo) {
        throw new BadRequestException('intervalo maximo permitido e de 3 meses');
      }
    }
    const normalizedSortBy = sortBy?.trim().toLowerCase();
    if (normalizedSortBy && normalizedSortBy !== 'datetime' && normalizedSortBy !== 'amount') {
      throw new BadRequestException('sortBy invalido');
    }
    const normalizedSortDir = sortDir?.trim().toLowerCase();
    if (normalizedSortDir && normalizedSortDir !== 'asc' && normalizedSortDir !== 'desc') {
      throw new BadRequestException('sortDir invalido');
    }
    const pageNumber = page ? Number(page) : 1;
    const rawLimit = limit ? Number(limit) : 100;
    const normalizeText = (value?: string) => (value ? value.trim() : undefined);
    const safeLimit =
      Number.isFinite(rawLimit) && rawLimit > 0 ? Math.min(Math.trunc(rawLimit), 5000) : 100;
    const includeReconciledBool =
      includeReconciled === '1' ||
      String(includeReconciled ?? '').trim().toLowerCase() === 'true';

    const data = acquirerList.length
      ? await this.acquirerImportService.listUnifiedSales({
          acquirers: acquirerList as Array<'cielo' | 'sipag' | 'sicredi'>,
          dateFrom,
          dateTo,
          page: Number.isNaN(pageNumber) || pageNumber < 1 ? 1 : pageNumber,
          limit: safeLimit,
          search: normalizeText(search),
          sortBy: normalizedSortBy as 'datetime' | 'amount' | undefined,
          sortDir: normalizedSortDir as 'asc' | 'desc' | undefined,
          includeReconciled: includeReconciledBool,
        })
      : normalized === 'cielo'
        ? await this.acquirerImportService.listCieloSales({
            dateFrom,
            dateTo,
            page: Number.isNaN(pageNumber) || pageNumber < 1 ? 1 : pageNumber,
            limit: safeLimit,
            status: normalizeText(status),
            brand: normalizeText(brand),
            search: normalizeText(search),
            paymentType: normalizeText(paymentType),
            sortBy: normalizedSortBy as 'datetime' | 'amount' | undefined,
            sortDir: normalizedSortDir as 'asc' | 'desc' | undefined,
            includeReconciled: includeReconciledBool,
          })
        : normalized === 'sipag'
          ? await this.acquirerImportService.listSipagSales({
              dateFrom,
              dateTo,
              page: Number.isNaN(pageNumber) || pageNumber < 1 ? 1 : pageNumber,
              limit: safeLimit,
              status: normalizeText(status),
              brand: normalizeText(brand),
              search: normalizeText(search),
              paymentType: normalizeText(paymentType),
              sortBy: normalizedSortBy as 'datetime' | 'amount' | undefined,
              sortDir: normalizedSortDir as 'asc' | 'desc' | undefined,
              includeReconciled: includeReconciledBool,
            })
          : await this.acquirerImportService.listSicrediSales({
              dateFrom,
              dateTo,
              page: Number.isNaN(pageNumber) || pageNumber < 1 ? 1 : pageNumber,
              limit: safeLimit,
              status: normalizeText(status),
              brand: normalizeText(brand),
              search: normalizeText(search),
              paymentType: normalizeText(paymentType),
              sortBy: normalizedSortBy as 'datetime' | 'amount' | undefined,
              sortDir: normalizedSortDir as 'asc' | 'desc' | undefined,
              includeReconciled: includeReconciledBool,
            });
    return { ok: true, data };
  }

  @Get('finance')
  async listFinance(
    @Query('acquirers') acquirers?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('search') search?: string,
    @Query('sortBy') sortBy?: string,
    @Query('sortDir') sortDir?: string,
    @Query('includeReconciled') includeReconciled?: string,
  ) {
    const acquirerList = acquirers
      ? acquirers
          .split(',')
          .map((value) => value.trim().toLowerCase())
          .filter((value) => value)
      : [];
    const validList = acquirerList.every(
      (value) => value === 'cielo' || value === 'sipag' || value === 'sicredi',
    );
    if (!acquirerList.length || !validList) {
      throw new BadRequestException('acquirers invalido');
    }
    if (!dateFrom || !/^\d{4}-\d{2}-\d{2}$/.test(dateFrom)) {
      throw new BadRequestException('dateFrom invalido');
    }
    if (!dateTo || !/^\d{4}-\d{2}-\d{2}$/.test(dateTo)) {
      throw new BadRequestException('dateTo invalido');
    }
    const from = new Date(`${dateFrom}T00:00:00.000Z`);
    const to = new Date(`${dateTo}T00:00:00.000Z`);
    if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime()) || to < from) {
      throw new BadRequestException('intervalo de datas invalido');
    }
    const maxTo = new Date(from);
    maxTo.setUTCMonth(maxTo.getUTCMonth() + 3);
    if (to > maxTo) {
      throw new BadRequestException('intervalo maximo permitido e de 3 meses');
    }
    const normalizedSortBy = sortBy?.trim().toLowerCase();
    if (normalizedSortBy && normalizedSortBy !== 'datetime' && normalizedSortBy !== 'amount') {
      throw new BadRequestException('sortBy invalido');
    }
    const normalizedSortDir = sortDir?.trim().toLowerCase();
    if (normalizedSortDir && normalizedSortDir !== 'asc' && normalizedSortDir !== 'desc') {
      throw new BadRequestException('sortDir invalido');
    }
    const includeReconciledBool =
      includeReconciled === '1' ||
      String(includeReconciled ?? '').trim().toLowerCase() === 'true';

    const data = await this.acquirerImportService.listUnifiedSales({
      acquirers: acquirerList as Array<'cielo' | 'sipag' | 'sicredi'>,
      dateFrom,
      dateTo,
      search: search?.trim() || undefined,
      sortBy: normalizedSortBy as 'datetime' | 'amount' | undefined,
      sortDir: normalizedSortDir as 'asc' | 'desc' | undefined,
      includeReconciled: includeReconciledBool,
      fetchAll: true,
    });
    return { ok: true, data };
  }
}
