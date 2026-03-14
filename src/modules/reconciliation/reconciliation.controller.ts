import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Param,
  Post,
  Query,
  UseGuards,
  UsePipes,
  ValidationPipe,
} from '@nestjs/common';
import { ReconciliationService } from './reconciliation.service';
import { AdminGuard } from '../../auth/admin.guard';
import { parseBoolean, toFbDateString } from './reconciliation.utils';
import { ManualReconciliationDto } from './reconciliation.dto';
import { ReconciliationStatusService } from './reconciliation-status.service';

@Controller('admin/reconciliation')
@UseGuards(AdminGuard)
export class ReconciliationController {
  constructor(
    private readonly reconciliationService: ReconciliationService,
    private readonly reconciliationStatusService: ReconciliationStatusService,
  ) {}

  @Get('status')
  async status() {
    return { ok: true, data: this.reconciliationStatusService.getStatus() };
  }

  @Post('run')
  // Dry-run checks:
  // 1) curl "http://localhost:3000/admin/reconciliation/run?dryRun=true&verbose=true" -H "Authorization: Bearer <ADMIN_TOKEN>"
  // 2) SELECT COUNT(*) FROM T_RECONCILIATION WHERE CREATED_AT >= CURRENT_TIMESTAMP - 1/24;
  async run(
    @Query('acquirer') acquirer?: string,
    @Query('limit') limit?: string,
    @Query('dryRun') dryRun?: string,
    @Query('verbose') verbose?: string,
    @Query('maxDeletesPerRun') maxDeletesPerRun?: string,
    @Query('probe') probe?: string,
    @Query('interdataId') interdataId?: string,
  ) {
    const limitNumber = limit ? Number(limit) : undefined;
    const normalized = acquirer?.trim().toUpperCase();
    const acquirers: ('CIELO' | 'SIPAG' | 'SICREDI')[] | undefined =
      normalized === 'CIELO' || normalized === 'SIPAG' || normalized === 'SICREDI'
        ? [normalized]
        : undefined;
    const dryRunEnabled = parseBoolean(dryRun) ?? false;
    const verboseEnabled = parseBoolean(verbose) ?? false;
    const maxDeletes = maxDeletesPerRun ? Number(maxDeletesPerRun) : undefined;
    const probeEnabled = parseBoolean(probe) ?? false;
    const interdataIdNumber = interdataId ? Number(interdataId) : undefined;

    return this.reconciliationService.reconcile({
      limit: Number.isNaN(limitNumber ?? NaN) ? undefined : limitNumber,
      acquirers,
      dryRun: dryRunEnabled,
      verbose: verboseEnabled,
      maxDeletesPerRun: Number.isNaN(maxDeletes ?? NaN) ? undefined : maxDeletes,
      probe: probeEnabled,
      interdataId: Number.isNaN(interdataIdNumber ?? NaN) ? undefined : interdataIdNumber,
    });
  }

  @Get('list')
  async list(
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('acquirer') acquirer?: string,
    @Query('status') status?: string,
    @Query('paymentType') paymentType?: string,
    @Query('brand') brand?: string,
    @Query('search') search?: string,
    @Query('limit') limit?: string,
    @Query('sortBy') sortBy?: string,
    @Query('sortDir') sortDir?: string,
  ) {
    if (dateFrom && !/^\d{4}-\d{2}-\d{2}$/.test(dateFrom)) {
      throw new BadRequestException('dateFrom invalido');
    }
    if (dateTo && !/^\d{4}-\d{2}-\d{2}$/.test(dateTo)) {
      throw new BadRequestException('dateTo invalido');
    }
    const normalizedSortBy = sortBy?.trim().toLowerCase();
    if (normalizedSortBy && normalizedSortBy !== 'datetime' && normalizedSortBy !== 'amount') {
      throw new BadRequestException('sortBy invalido');
    }
    const normalizedSortDir = sortDir?.trim().toLowerCase();
    if (normalizedSortDir && normalizedSortDir !== 'asc' && normalizedSortDir !== 'desc') {
      throw new BadRequestException('sortDir invalido');
    }
    const parsedLimit = limit ? Number(limit) : 2000;
    const safeLimit = Number.isFinite(parsedLimit) ? Math.min(Math.max(parsedLimit, 1), 20000) : 2000;

    const data = await this.reconciliationService.listReconciled({
      dateFrom,
      dateTo,
      acquirer,
      status: status?.trim() || undefined,
      paymentType: paymentType?.trim() || undefined,
      brand: brand?.trim() || undefined,
      search: search?.trim() || undefined,
      limit: safeLimit,
      sortBy: normalizedSortBy as 'datetime' | 'amount' | undefined,
      sortDir: normalizedSortDir as 'asc' | 'desc' | undefined,
    });
    return { ok: true, data };
  }

  @Get('duplicates')
  // curl "http://localhost:3000/admin/reconciliation/duplicates?date=2026-01-15&methodGroup=CARD&amount=11.98&tolerance=0.01&limit=200&includeReconciled=true" -H "Authorization: Bearer <ADMIN_TOKEN>"
  async duplicates(
    @Query('date') date?: string,
    @Query('amount') amount?: string,
    @Query('methodGroup') methodGroup?: string,
    @Query('tolerance') tolerance?: string,
    @Query('minutes') minutes?: string,
    @Query('baseDatetime') baseDatetime?: string,
    @Query('terminal') terminal?: string,
    @Query('brand') brand?: string,
    @Query('includeReconciled') includeReconciled?: string,
    @Query('limit') limit?: string,
    @Query('verbose') verbose?: string,
  ) {
    if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      throw new BadRequestException('date invalido');
    }
    if (!amount || Number.isNaN(Number(amount))) {
      throw new BadRequestException('amount invalido');
    }
    const normalizedMethod = methodGroup?.trim().toUpperCase();
    if (!normalizedMethod || (normalizedMethod !== 'CARD' && normalizedMethod !== 'PIX')) {
      throw new BadRequestException('methodGroup invalido');
    }
    if (baseDatetime && !/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(baseDatetime)) {
      throw new BadRequestException('baseDatetime invalido');
    }
    const toleranceValue = tolerance ? Number(tolerance) : 0.01;
    if (Number.isNaN(toleranceValue) || toleranceValue < 0) {
      throw new BadRequestException('tolerance invalido');
    }
    const minutesValue = minutes ? Number(minutes) : 180;
    if (Number.isNaN(minutesValue) || minutesValue < 0) {
      throw new BadRequestException('minutes invalido');
    }
    const rawLimit = limit ? Number(limit) : 100;
    const safeLimit = Math.min(Math.max(rawLimit, 1), 500);
    const includeReconciledFlag = parseBoolean(includeReconciled) ?? false;
    const verboseEnabled = parseBoolean(verbose) ?? false;

    const data = await this.reconciliationService.findDuplicates({
      date,
      amount: Number(amount),
      methodGroup: normalizedMethod as 'CARD' | 'PIX',
      tolerance: toleranceValue,
      minutes: minutesValue,
      baseDatetime,
      terminal: terminal?.trim() || undefined,
      brand: brand?.trim() || undefined,
      includeReconciled: includeReconciledFlag,
      limit: safeLimit,
      verbose: verboseEnabled,
    });
    return { ok: true, data };
  }

  @Get('pending')
  // curl "http://localhost:3000/admin/reconciliation/pending?source=INTERDATA&date=2026-01-15&limit=50" -H "Authorization: Bearer <ADMIN_TOKEN>"
  async pending(
    @Query('source') source?: string,
    @Query('date') date?: string,
    @Query('sortBy') sortBy?: string,
    @Query('sortDir') sortDir?: string,
  ) {
    const normalizedSource = source?.trim().toUpperCase();
    if (!normalizedSource) {
      throw new BadRequestException('source invalido');
    }
    if (
      normalizedSource !== 'INTERDATA' &&
      normalizedSource !== 'CIELO' &&
      normalizedSource !== 'SIPAG' &&
      normalizedSource !== 'SICREDI'
    ) {
      throw new BadRequestException('source invalido');
    }
    if (date && !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      throw new BadRequestException('date invalido');
    }
    const normalizedSortBy = sortBy?.trim().toLowerCase();
    if (normalizedSortBy && normalizedSortBy !== 'saledatetime' && normalizedSortBy !== 'grossamount') {
      throw new BadRequestException('sortBy invalido');
    }
    const normalizedSortDir = sortDir?.trim().toLowerCase();
    if (normalizedSortDir && normalizedSortDir !== 'asc' && normalizedSortDir !== 'desc') {
      throw new BadRequestException('sortDir invalido');
    }

    const data = await this.reconciliationService.listPending({
      source: normalizedSource as 'INTERDATA' | 'CIELO' | 'SIPAG' | 'SICREDI',
      date,
      sortBy: normalizedSortBy
        ? (normalizedSortBy === 'grossamount' ? 'grossAmount' : 'saleDatetime')
        : undefined,
      sortDir: normalizedSortDir as 'asc' | 'desc' | undefined,
    });
    return { ok: true, data };
  }

  @Get('pending/duplicates')
  // curl "http://localhost:3000/admin/reconciliation/pending/duplicates?source=CIELO&date=2026-01-15&limit=200" -H "Authorization: Bearer <ADMIN_TOKEN>"
  async pendingDuplicates(
    @Query('source') source?: string,
    @Query('date') date?: string,
    @Query('verbose') verbose?: string,
  ) {
    const normalizedSource = source?.trim().toUpperCase();
    if (!normalizedSource) {
      throw new BadRequestException('source invalido');
    }
    if (
      normalizedSource !== 'INTERDATA' &&
      normalizedSource !== 'CIELO' &&
      normalizedSource !== 'SIPAG' &&
      normalizedSource !== 'SICREDI'
    ) {
      throw new BadRequestException('source invalido');
    }
    if (date && !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      throw new BadRequestException('date invalido');
    }
    const verboseEnabled = parseBoolean(verbose) ?? false;

    const data = await this.reconciliationService.listPendingDuplicateGroups({
      source: normalizedSource as 'INTERDATA' | 'CIELO' | 'SIPAG' | 'SICREDI',
      date,
      verbose: verboseEnabled,
    });
    return { ok: true, data };
  }

  @Get('audit/duplicates')
  // curl "http://localhost:3000/admin/reconciliation/audit/duplicates?acquirer=cielo&from=2026-01-15&to=2026-01-15&onlySuspicious=true" -H "Authorization: Bearer <ADMIN_TOKEN>"
  async auditDuplicates(
    @Query('acquirer') acquirer?: string,
    @Query('from') from?: string,
    @Query('to') to?: string,
    @Query('onlySuspicious') onlySuspicious?: string,
  ) {
    const normalizedAcquirer = acquirer?.trim().toUpperCase() || 'ALL';
    if (
      normalizedAcquirer !== 'ALL' &&
      normalizedAcquirer !== 'CIELO' &&
      normalizedAcquirer !== 'SIPAG' &&
      normalizedAcquirer !== 'SICREDI'
    ) {
      throw new BadRequestException('acquirer invalido');
    }
    if (from && !/^\d{4}-\d{2}-\d{2}$/.test(from)) {
      throw new BadRequestException('from invalido');
    }
    if (to && !/^\d{4}-\d{2}-\d{2}$/.test(to)) {
      throw new BadRequestException('to invalido');
    }
    const onlySuspiciousFlag = parseBoolean(onlySuspicious) ?? true;

    const today = new Date();
    const toDate = to ? new Date(`${to}T00:00:00`) : today;
    const fromDate = from
      ? new Date(`${from}T00:00:00`)
      : new Date(toDate.getFullYear(), toDate.getMonth(), toDate.getDate() - 6);
    const fromValue = toFbDateString(fromDate);
    const toValue = toFbDateString(toDate);

    const data = await this.reconciliationService.auditDuplicates({
      acquirer: normalizedAcquirer as 'CIELO' | 'SIPAG' | 'SICREDI' | 'ALL',
      from: fromValue,
      to: toValue,
      onlySuspicious: onlySuspiciousFlag,
    });
    return { ok: true, data };
  }

  @Get(':id/details')
  async details(@Param('id') id: string) {
    const parsed = Number(id);
    if (!id || Number.isNaN(parsed)) {
      throw new BadRequestException('id invalido');
    }
    const data = await this.reconciliationService.getReconciliationDetails(parsed);
    return { ok: true, data };
  }

  @Post('manual')
  @UsePipes(
    new ValidationPipe({
      whitelist: true,
      transform: true,
      forbidNonWhitelisted: true,
    }),
  )
  async manual(@Body() body: ManualReconciliationDto) {
    return this.reconciliationService.reconcileManual(body);
  }
}
