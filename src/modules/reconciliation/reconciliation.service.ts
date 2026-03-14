import {
  BadRequestException,
  ConflictException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { hostname } from 'os';
import { DbService } from '../../db/db.service';
import { ReconciliationRepository } from './reconciliation.repository';
import { LockService } from '../../common/lock/lock.service';
import { ReconciliationMatchRepository } from './reconciliation-match.repository';
import {
  AMOUNT_TOLERANCE,
  DEFAULT_AMOUNT_TOLERANCE,
  CardType,
  PaymentMethod,
  PIX_TIME_TOLERANCE_MINUTES,
  amountDiff,
  buildDayWindow,
  buildTimeWindow,
  diffSeconds,
  hasTimeComponent,
  isCancelledStatus,
  isSameDay,
  isWithinAmountTolerance,
  isWithinTimeTolerance,
  normalizeBrand,
  normalizeIdentifier,
  normalizePaymentMethod,
  normalizeNsu,
  toCents,
  diffMinutes,
  normalizePaymentDetailsFromValues,
  stripLeadingZeros,
  timeDiffMs,
  toFbDateString,
  toFbTimestampString,
} from './reconciliation.utils';
import {
  AuditDuplicateRow,
  AuditDuplicateRowRaw,
  DuplicateGroup,
  ManualReconciliationDto,
  ManualReconciliationProvider,
} from './reconciliation.dto';
import { canonTerminal } from './canon/canonize';
import { ReconciliationStatusService } from './reconciliation-status.service';

type ReconcileParams = {
  limit?: number;
  acquirers?: ('CIELO' | 'SIPAG' | 'SICREDI')[];
  dryRun?: boolean;
  verbose?: boolean;
  maxDeletesPerRun?: number;
  probe?: boolean;
  interdataId?: number;
  dateFrom?: string;
  dateTo?: string;
};

type InterdataSale = {
  ID: number;
  SOURCE?: string;
  SALE_NO?: string;
  SALE_DATETIME?: Date;
  AUTH_NSU?: string;
  NSU?: string;
  AUTH_CODE?: string;
  TID?: string;
  PAYMENT_METHOD?: string;
  PAYMENT_TYPE?: string;
  CARD_MODE?: string;
  CARD_BRAND_RAW?: string;
  BRAND?: string;
  GROSS_AMOUNT?: number;
  NET_AMOUNT?: number;
  STATUS_RAW?: string;
  IS_CANCELLED?: number;
  CANON_SALE_DATE?: string;
  CANON_METHOD?: string;
  CANON_METHOD_GROUP?: string;
  CANON_GROSS_AMOUNT?: number;
  CANON_TERMINAL_NO?: string;
  CANON_AUTH_CODE?: string;
  CANON_NSU?: string;
  CANON_INSTALLMENT_NO?: number;
  CANON_INSTALLMENT_TOTAL?: number;
  CANON_BRAND?: string;
};

type CieloSale = {
  ID: number;
  SALE_DATETIME?: Date;
  AUTH_CODE?: string;
  NSU_DOC?: string;
  TID?: string;
  SALE_CODE?: string;
  BRAND?: string;
  PAYMENT_METHOD?: string;
  MODALITY?: string;
  GROSS_AMOUNT?: number;
  NET_AMOUNT?: number;
  STATUS?: string;
  CANON_SALE_DATE?: string;
  CANON_METHOD?: string;
  CANON_METHOD_GROUP?: string;
  CANON_GROSS_AMOUNT?: number;
  CANON_TERMINAL_NO?: string;
  CANON_AUTH_CODE?: string;
  CANON_NSU?: string;
  CANON_INSTALLMENT_NO?: number;
  CANON_INSTALLMENT_TOTAL?: number;
  CANON_BRAND?: string;
};

type SipagSale = {
  ID: number;
  SALE_DATETIME?: Date;
  TRANSACTION_NO?: string;
  AUTH_NO?: string;
  TERMINAL_NO?: string;
  BRAND?: string;
  PAYMENT_METHOD?: string;
  CREDIT_DEBIT_IND?: string;
  SALE_ID?: string;
  YOUR_NUMBER?: string;
  GROSS_AMOUNT?: number;
  NET_AMOUNT?: number;
  STATUS?: string;
  CANON_SALE_DATE?: string;
  CANON_METHOD?: string;
  CANON_METHOD_GROUP?: string;
  CANON_GROSS_AMOUNT?: number;
  CANON_TERMINAL_NO?: string;
  CANON_AUTH_CODE?: string;
  CANON_NSU?: string;
  CANON_INSTALLMENT_NO?: number;
  CANON_INSTALLMENT_TOTAL?: number;
  CANON_BRAND?: string;
};

type SicrediSale = {
  ID: number;
  SALE_DATETIME?: Date;
  NSU?: string;
  AUTH_CODE?: string;
  TERMINAL_NO?: string;
  BRAND?: string;
  PAYMENT_METHOD?: string;
  GROSS_AMOUNT?: number;
  NET_AMOUNT?: number;
  STATUS?: string;
  CANON_SALE_DATE?: string;
  CANON_METHOD?: string;
  CANON_METHOD_GROUP?: string;
  CANON_GROSS_AMOUNT?: number;
  CANON_TERMINAL_NO?: string;
  CANON_AUTH_CODE?: string;
  CANON_NSU?: string;
  CANON_INSTALLMENT_NO?: number;
  CANON_INSTALLMENT_TOTAL?: number;
  CANON_BRAND?: string;
};

type ReconRuleApplied = 'auto_pix_tolerance' | 'exact_match' | 'manual';
type MatchFlag = 'TIME_DIFF' | 'PIX_RULE_APPLIED' | 'MULTI_MATCH' | 'AMOUNT_DIFF_SMALL';

@Injectable()
export class ReconciliationService {
  private readonly logger = new Logger(ReconciliationService.name);
  private readonly instanceId = `${hostname()}:${process.pid}`;
  private cancelledCleanupInFlight: Promise<{ total: number }> | null = null;
  private cancelledCleanupLastAt = 0;

  constructor(
    private readonly dbService: DbService,
    private readonly repository: ReconciliationRepository,
    private readonly lockService: LockService,
    private readonly matchRepository: ReconciliationMatchRepository,
    private readonly reconciliationStatusService: ReconciliationStatusService,
  ) {}

  async reconcile(params: ReconcileParams) {
    return this.reconcileAll(params);
  }

  async reconcileAll(params: ReconcileParams) {
    await this.cleanupCancelledReconciliations();
    if (params.probe) {
      const interdataId = params.interdataId;
      if (!interdataId || Number.isNaN(Number(interdataId))) {
        throw new BadRequestException('interdataId invalido');
      }
      const acquirers: ('CIELO' | 'SIPAG' | 'SICREDI')[] = params.acquirers?.length
        ? params.acquirers
        : ['CIELO', 'SIPAG', 'SICREDI'];
      return this.runProbe(interdataId, acquirers, Boolean(params.verbose));
    }
    try {
      const acquired = await this.lockService.acquire(
        'RECONCILIATION_RUN',
        900,
        this.instanceId,
      );
      if (!acquired) {
        throw new ConflictException('reconciliation_already_running');
      }

      const dryRun = Boolean(params.dryRun);
      const verbose = Boolean(params.verbose);
      const runId = Date.now();
      const limit = Math.min(Math.max(params.limit ?? 2000, 1), 5000);
      const acquirers: ('CIELO' | 'SIPAG' | 'SICREDI')[] = params.acquirers?.length
        ? params.acquirers
        : ['CIELO', 'SIPAG', 'SICREDI'];
      const sales = (await this.repository.listInterdataSales({
        limit,
        dateFrom: params.dateFrom,
        dateTo: params.dateTo,
      })) as InterdataSale[];

      if (verbose) {
        this.logger.log(
          `Conciliacao: interdata=${sales.length} limit=${limit} acquirers=${acquirers.join(',')} dryRun=${dryRun}`,
        );
      }
      this.reconciliationStatusService.start({
        source: params.probe ? 'probe' : params.interdataId ? 'manual' : 'automatic',
        total: sales.length,
        dryRun,
        verbose,
        message: `Conciliando ${sales.length} venda(s)...`,
      });

      let processedInterdata = 0;
      let matchedCielo = 0;
      let matchedSipag = 0;
      let matchedSicredi = 0;
      let insertedRecon = 0;
      let skippedDuplicates = 0;
      let pending = 0;
      let errors = 0;
      let skippedMissingAcquirerRow = 0;
      let pixMatchedNsu = 0;
      let pixMatchedDatetimeAmount = 0;
      let pixSkippedNoCandidates = 0;
      let pixSkippedAmbiguous = 0;
      let pixSkippedPaymentMismatch = 0;
      let canonMatchedL1 = 0;
      let canonMatchedL2 = 0;
      let canonMatchedPixNoTerminal = 0;
      let canonMatchedL3 = 0;
      let canonMatchedL4 = 0;
      const errorsSample: Array<{ id: number; message: string }> = [];
      const matchedPreview: Array<Record<string, unknown>> = [];
      let previewMatchedCielo = 0;
      let previewMatchedSipag = 0;
      let previewMatchedSicredi = 0;
      const sampleMatched: Array<Record<string, unknown>> = [];
      const samplePending: Array<Record<string, unknown>> = [];
      const sampleAmbiguous: Array<Record<string, unknown>> = [];

      for (const sale of sales) {
        processedInterdata += 1;
        if (processedInterdata === 1 || processedInterdata % 25 === 0 || processedInterdata === sales.length) {
          this.reconciliationStatusService.update({
            processed: processedInterdata,
            matched: dryRun
              ? previewMatchedCielo + previewMatchedSipag + previewMatchedSicredi
              : matchedCielo + matchedSipag + matchedSicredi,
            pending,
            errors,
            message: `Conciliando ${processedInterdata}/${sales.length} venda(s)...`,
          });
        }

        try {
          const baseDatetime = sale.SALE_DATETIME ?? (sale as { CREATED_AT?: Date }).CREATED_AT ?? null;
          if (!baseDatetime || typeof sale.CANON_GROSS_AMOUNT !== 'number') {
            pending += 1;
            if (verbose) {
              this.logger.warn(
                `Conciliacao skip: ID=${sale.ID} missing SALE_DATETIME/CANON_GROSS_AMOUNT`,
              );
            }
            continue;
          }
          if (sale.IS_CANCELLED === 1) {
            pending += 1;
            if (verbose) {
              this.logger.warn(`Conciliacao skip: ID=${sale.ID} cancelado`);
            }
            continue;
          }

          const result = await this.matchCascade(sale, acquirers, verbose, {
            pixMatchedNsu: () => {
              pixMatchedNsu += 1;
            },
            pixMatchedDatetimeAmount: () => {
              pixMatchedDatetimeAmount += 1;
            },
            pixSkippedNoCandidates: () => {
              pixSkippedNoCandidates += 1;
            },
            pixSkippedAmbiguous: () => {
              pixSkippedAmbiguous += 1;
            },
            pixSkippedPaymentMismatch: () => {
              pixSkippedPaymentMismatch += 1;
            },
            canonMatchedL1: () => {
              canonMatchedL1 += 1;
            },
            canonMatchedL2: () => {
              canonMatchedL2 += 1;
            },
            canonMatchedPixNoTerminal: () => {
              canonMatchedPixNoTerminal += 1;
            },
            canonMatchedL3: () => {
              canonMatchedL3 += 1;
            },
            canonMatchedL4: () => {
              canonMatchedL4 += 1;
            },
          });
          if (!result.match) {
            pending += 1;
            if (!dryRun) {
              await this.upsertReconStatus({
                interdataId: sale.ID,
                runId,
                reason: result.reason ?? 'sem_match',
                details: result.details ?? {
                  interdataId: sale.ID,
                  canonDate: sale.CANON_SALE_DATE ?? null,
                  methodGroup: sale.CANON_METHOD_GROUP ?? null,
                  grossAmount: sale.CANON_GROSS_AMOUNT ?? null,
                },
              });
            } else if (verbose) {
              if (samplePending.length < 3) {
                samplePending.push({
                  interdataId: sale.ID,
                  reason: result.reason ?? 'sem_match',
                  details: result.details ?? null,
                });
              }
              if (result.reason?.includes('ambigu') && sampleAmbiguous.length < 3) {
                sampleAmbiguous.push({
                  interdataId: sale.ID,
                  reason: result.reason,
                  details: result.details ?? null,
                });
              }
            }
            if (verbose && result.reason) {
              this.logger.log(`Conciliacao sem match: ID=${sale.ID} motivo=${result.reason}`);
            }
            continue;
          }

          const match = result.match;
          const acquirerSale = match.candidate;
          if (!acquirerSale) {
            pending += 1;
            skippedMissingAcquirerRow += 1;
            const message = `Conciliacao skip: ID=${sale.ID} sem linha adquirente acq=${match.acquirer}:${match.acquirerId}`;
            this.logger.warn(message);
            if (verbose) {
              this.logger.log(message);
            }
            continue;
          }
          const statusBase = match.acqStatus ?? sale.STATUS_RAW ?? 'AUTO';
          const status = dryRun ? `DRY:${statusBase}` : statusBase;

          if (dryRun) {
            const isDuplicate = await this.dbService.transaction(async (tx) => {
              const interExists = await this.repository.existsReconciliationByInterdata(
                tx,
                sale.ID,
              );
              const acqExists = await this.repository.existsReconciliationByAcquirer(
                tx,
                match.acquirer,
                match.acquirerId,
              );
              return interExists || acqExists;
            });
            if (isDuplicate) {
              skippedDuplicates += 1;
              if (verbose) {
                this.logger.log(
                  `Conciliacao dryRun skip duplicado: ID=${sale.ID} acq=${match.acquirer}:${match.acquirerId}`,
                );
              }
              continue;
            }
            if (match.acquirer === 'CIELO') {
              previewMatchedCielo += 1;
            } else if (match.acquirer === 'SIPAG') {
              previewMatchedSipag += 1;
            } else {
              previewMatchedSicredi += 1;
            }
            if (sampleMatched.length < 3) {
              sampleMatched.push({
                interdataId: sale.ID,
                acquirer: match.acquirer,
                acquirerId: match.acquirerId,
                matchType: match.matchType,
                matchScore: match.matchScore,
                matchReason: match.matchReason ?? null,
              });
            }
            const interdataDatetime =
              sale.SALE_DATETIME ?? (sale as { CREATED_AT?: Date }).CREATED_AT ?? null;
            const deltaMinutes =
              interdataDatetime && match.acqSaleDatetime
                ? Math.abs(diffMinutes(interdataDatetime, match.acqSaleDatetime))
                : null;
            matchedPreview.push({
              interdataId: sale.ID,
              interdataDatetime,
              interdataAmount: sale.GROSS_AMOUNT ?? sale.CANON_GROSS_AMOUNT ?? null,
              interdataMethod:
                sale.CANON_METHOD_GROUP ??
                sale.CANON_METHOD ??
                sale.PAYMENT_METHOD ??
                sale.PAYMENT_TYPE ??
                null,
              acquirer: match.acquirer,
              acquirerId: match.acquirerId,
              acquirerDatetime: match.acqSaleDatetime ?? null,
              acquirerGross: match.acqGrossAmount ?? null,
              acquirerNet: match.acqNetAmount ?? null,
              matchReason: match.matchReason ?? match.matchType ?? null,
              deltaMinutes,
            });
            continue;
          }

          if (match.acquirer === 'CIELO') {
            matchedCielo += 1;
          } else if (match.acquirer === 'SIPAG') {
            matchedSipag += 1;
          } else {
            matchedSicredi += 1;
          }

          const txResult = await this.dbService.transaction(async (tx) => {
            const exists = await this.repository.existsReconciliation(
              tx,
              sale.ID,
              match.acquirer,
              match.acquirerId,
            );
            if (exists) {
              return { skippedDuplicate: true };
            }

            const acqProvider = match.acquirer ? match.acquirer.toLowerCase() : null;
            const payload = [
              sale.ID,
              match.acquirerId,
              match.acquirer,
              acqProvider,
              sale.SALE_NO ?? sale.NSU ?? null,
              sale.AUTH_NSU ?? sale.NSU ?? null,
              match.acqAuthCode ?? null,
              match.acqNsu ?? null,
              sale.SALE_DATETIME ?? null,
              match.acqSaleDatetime ?? null,
              sale.GROSS_AMOUNT ?? null,
              match.acqGrossAmount ?? null,
              sale.NET_AMOUNT ?? null,
              match.acqNetAmount ?? null,
              status,
              match.matchType,
              match.matchScore,
              match.amountDiff,
              new Date(),
            ];

            const paymentMethod = (acquirerSale as any)?.PAYMENT_METHOD ?? sale?.PAYMENT_METHOD ?? null;
            const paymentType = sale?.PAYMENT_TYPE ?? (acquirerSale as any)?.MODALITY ?? null;
            const cardMode = sale?.CARD_MODE ?? (acquirerSale as any)?.CREDIT_DEBIT_IND ?? null;
            const brand = sale?.CARD_BRAND_RAW ?? sale?.BRAND ?? (acquirerSale as any)?.BRAND ?? null;
            const acqFeeRaw =
              (acquirerSale as any)?.CANON_FEE_AMOUNT ??
              (acquirerSale as any)?.FEE_AMOUNT ??
              (acquirerSale as any)?.MDR_AMOUNT ??
              null;
            const acqFeeAmount =
              typeof acqFeeRaw === 'number'
                ? acqFeeRaw
                : acqFeeRaw === null || typeof acqFeeRaw === 'undefined' || acqFeeRaw === ''
                  ? null
                  : Number(acqFeeRaw);
            const acqGrossForTax = match.acqGrossAmount ?? null;
            const acqPercRaw = (acquirerSale as any)?.CANON_PERC_TAXA ?? null;
            const acqPercTaxa =
              typeof acqPercRaw === 'number'
                ? acqPercRaw
                : acqPercRaw === null || typeof acqPercRaw === 'undefined' || acqPercRaw === ''
                  ? acqFeeAmount !== null &&
                    acqGrossForTax !== null &&
                    acqGrossForTax !== 0
                    ? Number(((acqFeeAmount / acqGrossForTax) * 100).toFixed(4))
                    : null
                  : Number(acqPercRaw);

            const extras: Record<string, unknown> = {
              PAYMENT_METHOD: paymentMethod,
              PAYMENT_TYPE: paymentType,
              CARD_MODE: cardMode,
              CARD_BRAND_RAW: sale?.CARD_BRAND_RAW ?? null,
              BRAND: brand,
              ACQ_PAYMENT_METHOD: (acquirerSale as any)?.PAYMENT_METHOD ?? null,
              ACQ_BRAND: (acquirerSale as any)?.BRAND ?? null,
              ACQ_FEE_AMOUNT: Number.isFinite(acqFeeAmount as number) ? acqFeeAmount : null,
              ACQ_PERC_TAXA: Number.isFinite(acqPercTaxa as number) ? acqPercTaxa : null,
              RECON_RULE_APPLIED: match.reconRuleApplied ?? 'exact_match',
              IS_ACTIVE: 1,
              RUN_ID: runId,
              CREATED_BY: 'AUTO',
              SOURCE_RUN_KIND: 'AUTO',
            };

            const matchInfo = this.getMatchLayerInfo(match);
            if (matchInfo) {
              extras.MATCH_LAYER = matchInfo.layer;
              extras.MATCH_CONFIDENCE = matchInfo.confidence;
              extras.MATCH_REASON = matchInfo.reason;
              extras.MATCHED_AT = new Date();
            }

            await this.repository.insertReconciliation(tx, payload, {
              source: 'AUTO',
              reason: null,
              notes: null,
            }, extras);

            const reconciliationId = await this.repository.getReconciliationId(
              tx,
              sale.ID,
              match.acquirer,
              match.acquirerId,
            );
            if (!reconciliationId) {
              throw new Error('reconciliation_id_missing');
            }

            const acqSaleId =
              typeof match.acquirerId === 'number' && Number.isFinite(match.acquirerId)
                ? match.acquirerId
                : null;
            const autoMeta = this.buildAutoMatchMeta(sale, match, {
              missingAcquirerId: acqSaleId === null,
            });
            await this.matchRepository.insertMatch(tx, {
              reconciliationId,
              interdataSaleId: sale.ID,
              acqProvider: match.acquirer,
              acqSaleId,
              matchRule: match.matchType || 'AUTO',
              matchMeta: autoMeta,
              matchLayer: matchInfo?.layer ?? null,
              matchConfidence: matchInfo?.confidence ?? null,
              matchReason: matchInfo?.reason ?? null,
              runId,
              erpSnapshot: this.safeJson({ ...sale }),
              acqSnapshot: this.safeJson({ ...(acquirerSale as any) }),
              metaJson: this.safeJson({
                rule: match.matchType,
                reconRuleApplied: match.reconRuleApplied ?? 'exact_match',
                score: match.matchScore,
                amountDiff: match.amountDiff,
                amountDiffAbs: Math.abs(match.amountDiff ?? 0),
                amountDiffSmall: match.flags?.includes('AMOUNT_DIFF_SMALL') ?? false,
                timeDiffMinutes: match.timeDiffMinutes ?? null,
                candidateCount: match.candidateCount ?? null,
                flags: match.flags ?? [],
                layer: matchInfo?.layer ?? null,
                reason: matchInfo?.reason ?? null,
              }),
            });

            return { inserted: true };
          });

          if (txResult?.skippedDuplicate) {
            skippedDuplicates += 1;
          } else if (txResult?.inserted) {
            insertedRecon += 1;
            if (verbose && sampleMatched.length < 3) {
              sampleMatched.push({
                interdataId: sale.ID,
                acquirer: match.acquirer,
                acquirerId: match.acquirerId,
                matchType: match.matchType,
                matchScore: match.matchScore,
                matchReason: match.matchReason ?? null,
              });
            }
          }

          if (verbose) {
            this.logger.log(
              `Conciliacao match: ID=${sale.ID} acq=${match.acquirer} acqId=${match.acquirerId} type=${match.matchType} score=${match.matchScore} regra=${match.reconRuleApplied ?? 'exact_match'} diffMin=${match.timeDiffMinutes ?? 'N/A'} diffValor=${Math.abs(match.amountDiff ?? 0).toFixed(2)} candidatos=${match.candidateCount ?? 1}`,
            );
          }
        } catch (error) {
          const err = error instanceof Error ? error : new Error('Erro desconhecido');
          const nsu = sale.SALE_NO ?? sale.NSU ?? sale.AUTH_NSU ?? 'N/A';
          const stack = err.stack ? err.stack.split('\n').slice(0, 5).join('\n') : undefined;
          this.logger.error(
            `Falha ao conciliar venda ${sale.ID} nsu=${nsu}`,
            stack,
          );
          errors += 1;
          if (errorsSample.length < 20) {
            errorsSample.push({ id: sale.ID, message: err.message });
          }
        }
      }

      const summary = {
        runId,
        processed: processedInterdata,
        matched: dryRun
          ? previewMatchedCielo + previewMatchedSipag + previewMatchedSicredi
          : matchedCielo + matchedSipag + matchedSicredi,
        skipped: pending + skippedDuplicates,
        errors,
        errorsSample,
        matchedCielo: dryRun ? previewMatchedCielo : matchedCielo,
        matchedSipag: dryRun ? previewMatchedSipag : matchedSipag,
        matchedSicredi: dryRun ? previewMatchedSicredi : matchedSicredi,
        insertedRecon: dryRun ? 0 : insertedRecon,
        skippedDuplicates,
        pending,
        skippedMissingAcquirerRow,
        pixMatchedNsu,
        pixMatchedDatetimeAmount,
        pixSkippedNoCandidates,
        pixSkippedAmbiguous,
        pixSkippedPaymentMismatch,
        canonMatchedL1,
        canonMatchedL2,
        canonMatchedPixNoTerminal,
        canonMatchedL3,
        canonMatchedL4,
      };

      if (verbose) {
        this.logger.log(
          `Canon layers: L1=${canonMatchedL1} L2=${canonMatchedL2} PixNoTerminal=${canonMatchedPixNoTerminal} L3=${canonMatchedL3} L4=${canonMatchedL4}`,
        );
      }

      if (dryRun) {
        const payload = {
          runId,
          params,
          summary,
          samples: {
            matched: sampleMatched,
            pending: samplePending,
            ambiguous: sampleAmbiguous,
          },
        };
        await this.storeReconResult(runId, payload);
      }

      this.logger.log(`Conciliacao resumo: ${JSON.stringify(summary)}`);
      this.reconciliationStatusService.finish(
        summary,
        `Conciliacao finalizada: ${processedInterdata}/${sales.length} processadas.`,
      );
      return {
        ok: true,
        data: {
          ...summary,
          matchedPreview: dryRun ? matchedPreview : undefined,
          samples: verbose
            ? { matched: sampleMatched, pending: samplePending, ambiguous: sampleAmbiguous }
            : undefined,
        },
      };
    } catch (error) {
      const err = error instanceof Error ? error : new Error('Erro na conciliacao');
      this.reconciliationStatusService.fail(
        { error: err.message },
        `Conciliacao interrompida: ${err.message}`,
      );
      throw error;
    } finally {
      await this.lockService.release('RECONCILIATION_RUN', this.instanceId);
    }
  }

  async hasPendingInterdataSales(options?: { dateFrom?: string; dateTo?: string }): Promise<boolean> {
    const conditions: string[] = [];
    const params: unknown[] = [];
    if (options?.dateFrom) {
      conditions.push('SALE_DATETIME >= ?');
      params.push(`${options.dateFrom} 00:00:00`);
    }
    if (options?.dateTo) {
      const [year, month, day] = options.dateTo.split('-').map(Number);
      const nextDay = new Date(year, month - 1, day + 1, 0, 0, 0, 0);
      const yyyy = nextDay.getFullYear();
      const mm = String(nextDay.getMonth() + 1).padStart(2, '0');
      const dd = String(nextDay.getDate()).padStart(2, '0');
      params.push(`${yyyy}-${mm}-${dd} 00:00:00`);
      conditions.push('SALE_DATETIME < ?');
    }
    const where = conditions.length ? ` WHERE ${conditions.join(' AND ')}` : '';
    const rows = await this.dbService.query<{ TOTAL: number }>(
      `SELECT COUNT(*) AS TOTAL FROM V_PENDING_INTERDATA${where}`,
      params,
    );
    const total = rows[0]?.TOTAL ?? 0;
    return total > 0;
  }

  async listReconciled(options: {
    dateFrom?: string;
    dateTo?: string;
    acquirer?: string;
    status?: string;
    paymentType?: string;
    brand?: string;
    search?: string;
    limit?: number;
    sortBy?: 'datetime' | 'amount';
    sortDir?: 'asc' | 'desc';
  }) {
    await this.cleanupCancelledReconciliations();
    const conditions: string[] = [];
    const params: unknown[] = [];
    const normalizeUpperAscii = (value: string) =>
      value
        .trim()
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toUpperCase();

    if (options.dateFrom) {
      conditions.push('r.SALE_DATETIME >= ?');
      params.push(`${options.dateFrom} 00:00:00`);
    }
    if (options.dateTo) {
      const [year, month, day] = options.dateTo.split('-').map(Number);
      const nextDay = new Date(year, month - 1, day);
      nextDay.setDate(nextDay.getDate() + 1);
      conditions.push('r.SALE_DATETIME < ?');
      params.push(toFbTimestampString(nextDay));
    }
    if (options.acquirer) {
      conditions.push('UPPER(r.ACQUIRER) LIKE ?');
      params.push(`%${options.acquirer.trim().toUpperCase()}%`);
    }
    if (options.status) {
      conditions.push(
        "UPPER(COALESCE(c.STATUS, s.STATUS, si.STATUS, si.PAY_STATUS, r.STATUS, '')) LIKE ?",
      );
      params.push(`%${options.status.trim().toUpperCase()}%`);
    }
    if (options.paymentType) {
      const payment = normalizeUpperAscii(options.paymentType);
      const paymentNeedle =
        payment.includes('CARTAO') || payment === 'CARD'
          ? 'CARD'
          : payment.includes('PIX')
            ? 'PIX'
            : payment;
      conditions.push(
        "(UPPER(COALESCE(c.CANON_METHOD_GROUP, s.CANON_METHOD_GROUP, si.CANON_METHOD_GROUP, '')) LIKE ? OR UPPER(COALESCE(c.CANON_METHOD, s.CANON_METHOD, si.CANON_METHOD, c.PAYMENT_METHOD, s.PAYMENT_METHOD, si.PRODUCT, '')) LIKE ?)",
      );
      params.push(`%${paymentNeedle}%`, `%${paymentNeedle}%`);
    }
    if (options.brand) {
      const brand = normalizeUpperAscii(options.brand);
      const methodNeedles: string[] = [];
      if (brand.includes('DEBITO') || brand.includes('DEBIT')) {
        methodNeedles.push('DEBITO', 'DEBIT');
      } else if (brand.includes('CREDITO') || brand.includes('CREDIT') || brand.includes('CRED')) {
        methodNeedles.push('CREDITO', 'CREDIT', 'CREDITO_A_VISTA');
      } else if (brand.includes('VOUCHER')) {
        methodNeedles.push('VOUCHER');
      } else if (brand.includes('PIX')) {
        methodNeedles.push('PIX');
      }

      if (methodNeedles.length) {
        const methodConditions = methodNeedles
          .map(() => "UPPER(COALESCE(c.CANON_METHOD, s.CANON_METHOD, si.CANON_METHOD, '')) LIKE ?")
          .join(' OR ');
        conditions.push(
          `(${methodConditions} OR UPPER(COALESCE(c.CANON_BRAND, s.CANON_BRAND, si.CANON_BRAND, c.BRAND, s.BRAND, si.BRAND, '')) LIKE ?)`,
        );
        params.push(...methodNeedles.map((needle) => `%${needle}%`), `%${brand}%`);
      } else {
        conditions.push(
          "(UPPER(COALESCE(c.CANON_BRAND, s.CANON_BRAND, si.CANON_BRAND, c.BRAND, s.BRAND, si.BRAND, '')) LIKE ? OR UPPER(COALESCE(c.CANON_METHOD, s.CANON_METHOD, si.CANON_METHOD, '')) LIKE ?)",
        );
        params.push(`%${brand}%`, `%${brand}%`);
      }
    }
    if (options.search) {
      const needle = `%${options.search.trim().toUpperCase()}%`;
      conditions.push(`(
        UPPER(COALESCE(r.SALE_NO, '')) LIKE ?
        OR UPPER(COALESCE(r.ACQ_NSU, '')) LIKE ?
        OR UPPER(COALESCE(r.ACQ_AUTH_CODE, '')) LIKE ?
        OR UPPER(COALESCE(c.NSU_DOC, s.TRANSACTION_NO, si.NSU, '')) LIKE ?
        OR UPPER(COALESCE(c.AUTH_CODE, s.AUTH_NO, si.AUTH_CODE, '')) LIKE ?
      )`);
      params.push(needle, needle, needle, needle, needle);
    }
    conditions.push('COALESCE(r.IS_ACTIVE, 1) = 1');

    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const orderBy = this.buildOrderBy(options.sortBy, options.sortDir, {
      datetime: 'r.SALE_DATETIME',
      amount: 'COALESCE(r.ACQ_GROSS_AMOUNT, r.GROSS_AMOUNT)',
    });
    const limit = options.limit ? Math.min(Math.max(options.limit, 1), 20000) : 2000;
    const sql = `
      SELECT FIRST ${limit}
        r.*,
        COALESCE(c.CANON_METHOD_GROUP, s.CANON_METHOD_GROUP, si.CANON_METHOD_GROUP) AS ACQ_CANON_METHOD_GROUP_RESOLVED,
        COALESCE(c.CANON_METHOD, s.CANON_METHOD, si.CANON_METHOD) AS ACQ_CANON_METHOD_RESOLVED,
        COALESCE(c.CANON_BRAND, s.CANON_BRAND, si.CANON_BRAND) AS ACQ_CANON_BRAND_RESOLVED,
        COALESCE(c.PAYMENT_METHOD, s.PAYMENT_METHOD, si.PRODUCT) AS ACQ_PAYMENT_METHOD_RESOLVED,
        COALESCE(c.BRAND, s.BRAND, si.BRAND) AS ACQ_BRAND_RESOLVED,
        COALESCE(c.STATUS, s.STATUS, si.STATUS, si.PAY_STATUS) AS ACQ_STATUS_RESOLVED
      FROM T_RECONCILIATION r
      LEFT JOIN T_CIELO_SALES c
        ON UPPER(COALESCE(r.ACQUIRER, '')) = 'CIELO'
       AND c.ID = r.ACQUIRER_ID
      LEFT JOIN T_SIPAG_SALES s
        ON UPPER(COALESCE(r.ACQUIRER, '')) = 'SIPAG'
       AND s.ID = r.ACQUIRER_ID
      LEFT JOIN T_SICREDI_SALES si
        ON UPPER(COALESCE(r.ACQUIRER, '')) = 'SICREDI'
       AND si.ID = r.ACQUIRER_ID
      ${where}
      ORDER BY ${orderBy}
    `;
    if (process.env.NODE_ENV === 'development') {
      this.logger.log(`SQL listReconciled: ${sql}`);
    }
    return this.dbService.query(sql, params);
  }

  async getReconciliationDetails(id: number) {
    const reconciliationRows = await this.dbService.query<Record<string, any>>(
      'SELECT * FROM T_RECONCILIATION WHERE ID = ?',
      [id],
    );
    const reconciliation = reconciliationRows[0];
    if (!reconciliation) {
      throw new NotFoundException('reconciliation_not_found');
    }

    let match = await this.matchRepository.getByReconciliationId(id);
    if (!match) {
      const fallbackProvider =
        String(reconciliation.ACQ_PROVIDER ?? reconciliation.ACQUIRER ?? '').trim().toUpperCase() ||
        null;
      match = {
        RECONCILIATION_ID: id,
        INTERDATA_SALE_ID: reconciliation.INTERDATA_ID ?? null,
        ACQ_PROVIDER: fallbackProvider,
        ACQ_SALE_ID: reconciliation.ACQUIRER_ID ?? null,
        MATCH_RULE: 'LEGACY',
        MATCH_META: null,
      };
    }

    const interdataId = match.INTERDATA_SALE_ID ?? null;
    const interdata = interdataId
      ? (await this.dbService.query<Record<string, any>>(
          'SELECT * FROM T_INTERDATA_SALES WHERE ID = ?',
          [interdataId],
        ))[0] ?? null
      : null;

    const provider = String(match.ACQ_PROVIDER ?? '').trim().toUpperCase();
    const acqSaleId = match.ACQ_SALE_ID ?? null;
    let acquirerRow: unknown = null;
    if (acqSaleId) {
      if (provider === 'CIELO') {
        acquirerRow = (await this.dbService.query<Record<string, any>>(
          'SELECT * FROM T_CIELO_SALES WHERE ID = ?',
          [acqSaleId],
        ))[0] ?? null;
      } else if (provider === 'SIPAG') {
        acquirerRow = (await this.dbService.query<Record<string, any>>(
          'SELECT * FROM T_SIPAG_SALES WHERE ID = ?',
          [acqSaleId],
        ))[0] ?? null;
      } else if (provider === 'SICREDI') {
        acquirerRow = (await this.dbService.query<Record<string, any>>(
          'SELECT * FROM T_SICREDI_SALES WHERE ID = ?',
          [acqSaleId],
        ))[0] ?? null;
      } else if (provider) {
        throw new BadRequestException('acq_provider_invalid');
      }
    }

    if (process.env.DEBUG === 'true') {
      this.logger.debug(
        `Recon details: reconciliationId=${id} matchRow=${JSON.stringify({
          INTERDATA_SALE_ID: match.INTERDATA_SALE_ID ?? null,
          ACQ_PROVIDER: match.ACQ_PROVIDER ?? null,
          ACQ_SALE_ID: match.ACQ_SALE_ID ?? null,
        })} interdataFound=${Boolean(interdata)} acquirerFound=${Boolean(acquirerRow)}`,
      );
    }

    let matchMeta: Record<string, unknown> | string | null = null;
    if (match?.MATCH_META !== undefined && match?.MATCH_META !== null) {
      if (typeof match.MATCH_META === 'string') {
        try {
          matchMeta = JSON.parse(match.MATCH_META);
        } catch {
          matchMeta = match.MATCH_META;
        }
      } else {
        matchMeta = match.MATCH_META as any;
      }
    }

    const interdataUi = interdata
      ? {
          id: interdata.ID ?? reconciliation.INTERDATA_ID ?? null,
          pdvNsu: interdata.AUTH_NSU ?? null,
          saleNo: interdata.SALE_NO ?? reconciliation.SALE_NO ?? null,
          datetime: interdata.SALE_DATETIME ?? reconciliation.SALE_DATETIME ?? null,
          grossAmount: interdata.GROSS_AMOUNT ?? reconciliation.GROSS_AMOUNT ?? null,
          netAmount: interdata.NET_AMOUNT ?? reconciliation.NET_AMOUNT ?? null,
          brand: interdata.BRAND ?? reconciliation.BRAND ?? null,
          status: interdata.STATUS_RAW ?? interdata.STATUS ?? reconciliation.STATUS ?? null,
          paymentMethod: interdata.PAYMENT_METHOD ?? null,
        }
      : {
          id: reconciliation.INTERDATA_ID ?? match.INTERDATA_SALE_ID ?? null,
          pdvNsu: reconciliation.AUTH_NSU ?? null,
          saleNo: reconciliation.SALE_NO ?? null,
          datetime: reconciliation.SALE_DATETIME ?? null,
          grossAmount: reconciliation.GROSS_AMOUNT ?? null,
          netAmount: reconciliation.NET_AMOUNT ?? null,
          brand: reconciliation.BRAND ?? null,
          status: reconciliation.STATUS ?? null,
          paymentMethod: null,
        };

    const pickFirst = (row: Record<string, any> | null, keys: string[]) => {
      if (!row) {
        return null;
      }
      for (const key of keys) {
        const value = row[key];
        if (value !== undefined && value !== null && value !== '') {
          return value;
        }
      }
      return null;
    };

    const normalizeProvider = (value: unknown): 'CIELO' | 'SIPAG' | 'SICREDI' | '' => {
      const normalized = String(value ?? '').trim().toUpperCase();
      if (normalized === 'CIELO' || normalized === 'SIPAG' || normalized === 'SICREDI') {
        return normalized;
      }
      return '';
    };

    const acquirerRecord = acquirerRow as Record<string, any> | null;
    let acquirerUi: any = null;
    if (acquirerRecord) {
      if (provider === 'CIELO') {
        acquirerUi = {
          provider,
          id: acquirerRecord.ID ?? null,
          nsu: acquirerRecord.NSU_DOC ?? acquirerRecord.E_NSU_DOC ?? null,
          authCode: acquirerRecord.AUTH_CODE ?? acquirerRecord.E_AUTH_CODE ?? null,
          datetime:
            acquirerRecord.SALE_DATETIME ??
            acquirerRecord.E_AUTH_DATE ??
            acquirerRecord.E_CAPTURE_DATE ??
            acquirerRecord.CREATED_AT ??
            null,
          grossAmount: acquirerRecord.GROSS_AMOUNT ?? acquirerRecord.E_GROSS_AMOUNT ?? null,
          netAmount: acquirerRecord.NET_AMOUNT ?? acquirerRecord.E_NET_AMOUNT ?? null,
          brand: acquirerRecord.BRAND ?? acquirerRecord.E_AUTH_BRAND ?? null,
          status: acquirerRecord.STATUS ?? null,
          paymentMethod: acquirerRecord.PAYMENT_METHOD ?? null,
          extra: {
            saleCode: acquirerRecord.SALE_CODE ?? acquirerRecord.E_UNIQUE_SALE_CODE ?? null,
            tid: acquirerRecord.TID ?? acquirerRecord.E_TID ?? null,
          },
        };
      } else {
        acquirerUi = {
          provider,
          id: acquirerRecord.ID ?? null,
          nsu: pickFirst(acquirerRecord, [
            'NSU',
            'ACQ_NSU',
            'AUTH_NSU',
            'NSU_CIELO',
            'NSU_HOST',
            'NSU_ADQ',
          ]),
          authCode: pickFirst(acquirerRecord, [
            'AUTH_CODE',
            'ACQ_AUTH_CODE',
            'COD_AUTORIZACAO',
            'AUTHORIZATION_CODE',
          ]),
          datetime: pickFirst(acquirerRecord, [
            'SALE_DATETIME',
            'AUTH_DATETIME',
            'TRANSACTION_DATETIME',
            'CREATED_AT',
            'DT_HR_VENDA',
          ]),
          grossAmount: pickFirst(acquirerRecord, [
            'GROSS_AMOUNT',
            'ACQ_GROSS_AMOUNT',
            'AMOUNT',
            'VALOR_BRUTO',
          ]),
          netAmount: pickFirst(acquirerRecord, [
            'NET_AMOUNT',
            'ACQ_NET_AMOUNT',
            'NET',
            'VALOR_LIQUIDO',
          ]),
          brand: pickFirst(acquirerRecord, ['BRAND', 'CARD_BRAND', 'BANDEIRA']),
          status: pickFirst(acquirerRecord, ['STATUS_RAW', 'STATUS', 'SITUACAO']),
          paymentMethod: pickFirst(acquirerRecord, ['PAYMENT_METHOD', 'PAYMENT_TYPE', 'CARD_MODE']),
        };
      }
    }
    if (!acquirerUi) {
      const fallbackProvider = normalizeProvider(reconciliation.ACQUIRER ?? match.ACQ_PROVIDER);
      acquirerUi = {
        provider: fallbackProvider || provider,
        id: reconciliation.ACQUIRER_ID ?? match.ACQ_SALE_ID ?? null,
        nsu: reconciliation.ACQ_NSU ?? null,
        authCode: reconciliation.ACQ_AUTH_CODE ?? null,
        datetime: reconciliation.ACQ_SALE_DATETIME ?? null,
        grossAmount: reconciliation.ACQ_GROSS_AMOUNT ?? null,
        netAmount: reconciliation.ACQ_NET_AMOUNT ?? null,
        brand: reconciliation.ACQ_BRAND ?? reconciliation.BRAND ?? null,
        status: reconciliation.STATUS ?? null,
        paymentMethod: null,
      };
    }

    if (process.env.DEBUG === 'true') {
      const usedInterdataFallback = !interdata;
      const usedAcquirerFallback = !acquirerRow;
      this.logger.debug(
        `Recon details fallback: id=${id} match=${JSON.stringify({
          INTERDATA_SALE_ID: match.INTERDATA_SALE_ID ?? null,
          ACQ_PROVIDER: match.ACQ_PROVIDER ?? null,
          ACQ_SALE_ID: match.ACQ_SALE_ID ?? null,
        })} foundInterdataRow=${Boolean(interdata)} foundAcquirerRow=${Boolean(acquirerRow)} usedFallbacks=${JSON.stringify({ interdata: usedInterdataFallback, acquirer: usedAcquirerFallback })}`,
      );
    }

    const includeRaw = process.env.NODE_ENV !== 'production';
    return {
      reconciliation,
      match,
      matchMeta,
      interdataUi,
      acquirerUi,
      interdataRaw: includeRaw ? interdata : null,
      acquirerRaw: includeRaw ? acquirerRow : null,
    };
  }

  async findDuplicates(params: {
    date: string;
    amount: number;
    methodGroup: 'CARD' | 'PIX';
    tolerance: number;
    minutes: number;
    baseDatetime?: string;
    terminal?: string;
    brand?: string;
    includeReconciled: boolean;
    limit: number;
    verbose: boolean;
  }) {
    const baseDate = params.date;
    const parseBaseDatetime = (value: string) => {
      const match = value.match(
        /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/,
      );
      if (!match) {
        return null;
      }
      const [, y, m, d, hh, mm, ss] = match;
      const date = new Date(
        Number(y),
        Number(m) - 1,
        Number(d),
        Number(hh),
        Number(mm),
        Number(ss),
      );
      return Number.isNaN(date.getTime()) ? null : date;
    };
    const base = params.baseDatetime ? parseBaseDatetime(params.baseDatetime) : null;
    const minutesMs = params.minutes * 60 * 1000;
    const startDate = base
      ? new Date(base.getTime() - minutesMs)
      : new Date(`${baseDate}T00:00:00`);
    const endDate = base
      ? new Date(base.getTime() + minutesMs)
      : new Date(`${baseDate}T23:59:59`);
    const start = toFbTimestampString(startDate);
    const end = toFbTimestampString(endDate);

    const terminal = params.terminal?.trim();
    const brand = params.brand?.trim();
    const includeReconciled = params.includeReconciled;

    const buildWhere = (alias: string, excludeReconciled: boolean, acquirer?: 'CIELO' | 'SIPAG' | 'SICREDI') => {
      const conditions: string[] = [
        `${alias}.CANON_SALE_DATE = ?`,
        `${alias}.CANON_METHOD_GROUP = ?`,
        `ABS(${alias}.CANON_GROSS_AMOUNT - ?) <= ?`,
        `${alias}.SALE_DATETIME BETWEEN ? AND ?`,
      ];
      const values: unknown[] = [
        baseDate,
        params.methodGroup,
        params.amount,
        params.tolerance,
        start,
        end,
      ];

      if (terminal) {
        conditions.push(`TRIM(${alias}.CANON_TERMINAL_NO) = ?`);
        values.push(terminal);
      }
      if (brand) {
        conditions.push(`UPPER(${alias}.CANON_BRAND) = UPPER(?)`);
        values.push(brand);
      }
      if (excludeReconciled) {
        if (acquirer) {
          conditions.push(
            `NOT EXISTS (SELECT 1 FROM T_RECONCILIATION r WHERE r.ACQUIRER = '${acquirer}' AND r.ACQUIRER_ID = ${alias}.ID)`,
          );
        } else {
          conditions.push(
            `NOT EXISTS (SELECT 1 FROM T_RECONCILIATION r WHERE r.INTERDATA_ID = ${alias}.ID)`,
          );
        }
      }
      return { where: conditions.join(' AND '), values };
    };

    const interFilter = buildWhere('i', !includeReconciled);
    const cieloFilter = buildWhere('c', !includeReconciled, 'CIELO');
    const sipagFilter = buildWhere('s', !includeReconciled, 'SIPAG');
    const sicrediFilter = buildWhere('si', !includeReconciled, 'SICREDI');

    const interdata = await this.dbService.query(
      `SELECT FIRST ${params.limit} i.ID, i.SALE_NO, i.SALE_DATETIME, i.STATUS_RAW, i.CANON_SALE_DATE, i.CANON_METHOD_GROUP, i.CANON_METHOD, i.CANON_BRAND, i.CANON_TERMINAL_NO, i.CANON_GROSS_AMOUNT, i.AUTH_NSU FROM T_INTERDATA_SALES i WHERE ${interFilter.where} ORDER BY i.SALE_DATETIME`,
      interFilter.values,
    );
    const cielo = await this.dbService.query(
      `SELECT FIRST ${params.limit} c.ID, c.SALE_DATETIME, c.STATUS, c.CANON_SALE_DATE, c.CANON_METHOD_GROUP, c.CANON_METHOD, c.CANON_BRAND, c.CANON_TERMINAL_NO, c.CANON_GROSS_AMOUNT, c.CANON_AUTH_CODE, c.CANON_NSU FROM T_CIELO_SALES c WHERE ${cieloFilter.where} ORDER BY c.SALE_DATETIME`,
      cieloFilter.values,
    );
    const sipag = await this.dbService.query(
      `SELECT FIRST ${params.limit} s.ID, s.SALE_DATETIME, s.STATUS, s.CANON_SALE_DATE, s.CANON_METHOD_GROUP, s.CANON_METHOD, s.CANON_BRAND, s.CANON_TERMINAL_NO, s.CANON_GROSS_AMOUNT, s.CANON_AUTH_CODE, s.CANON_NSU FROM T_SIPAG_SALES s WHERE ${sipagFilter.where} ORDER BY s.SALE_DATETIME`,
      sipagFilter.values,
    );
    const sicredi = await this.dbService.query(
      `SELECT FIRST ${params.limit} si.ID, si.SALE_DATETIME, si.STATUS, si.CANON_SALE_DATE, si.CANON_METHOD_GROUP, si.CANON_METHOD, si.CANON_BRAND, si.CANON_TERMINAL_NO, si.CANON_GROSS_AMOUNT, si.CANON_AUTH_CODE, si.CANON_NSU FROM T_SICREDI_SALES si WHERE ${sicrediFilter.where} ORDER BY si.SALE_DATETIME`,
      sicrediFilter.values,
    );

    const formatAmountKey = (value: unknown) => {
      const num = typeof value === 'number' ? value : Number(value);
      return Number.isFinite(num) ? num.toFixed(2) : 'NA';
    };
    const keyFor = (row: any) =>
      `${row.CANON_SALE_DATE}|${row.CANON_METHOD_GROUP}|${formatAmountKey(row.CANON_GROSS_AMOUNT)}`;
    const groupsMap = new Map<
      string,
      {
        key: string;
        interdataIds: number[];
        cieloIds: number[];
        sipagIds: number[];
        sicrediIds: number[];
        totals: { interdata: number; cielo: number; sipag: number; sicredi: number };
      }
    >();
    const ensureGroup = (key: string) => {
      if (!groupsMap.has(key)) {
        groupsMap.set(key, {
          key,
          interdataIds: [],
          cieloIds: [],
          sipagIds: [],
          sicrediIds: [],
          totals: { interdata: 0, cielo: 0, sipag: 0, sicredi: 0 },
        });
      }
      return groupsMap.get(key)!;
    };

    interdata.forEach((row: any) => {
      const group = ensureGroup(keyFor(row));
      group.interdataIds.push(row.ID);
      group.totals.interdata += 1;
    });
    cielo.forEach((row: any) => {
      const group = ensureGroup(keyFor(row));
      group.cieloIds.push(row.ID);
      group.totals.cielo += 1;
    });
    sipag.forEach((row: any) => {
      const group = ensureGroup(keyFor(row));
      group.sipagIds.push(row.ID);
      group.totals.sipag += 1;
    });
    sicredi.forEach((row: any) => {
      const group = ensureGroup(keyFor(row));
      group.sicrediIds.push(row.ID);
      group.totals.sicredi += 1;
    });

    const counts = {
      interdata: interdata.length,
      cielo: cielo.length,
      sipag: sipag.length,
      sicredi: sicredi.length,
    };

    if (params.verbose) {
      this.logger.log(
        `Duplicates query: date=${baseDate} method=${params.methodGroup} amount=${params.amount} tol=${params.tolerance} range=${start}..${end} terminal=${terminal ?? 'N/A'} brand=${brand ?? 'N/A'} counts=${JSON.stringify(counts)}`,
      );
    }

    return {
      query: {
        date: baseDate,
        amount: params.amount,
        methodGroup: params.methodGroup,
        tolerance: params.tolerance,
        minutes: params.minutes,
        baseDatetime: params.baseDatetime ?? null,
        terminal: terminal ?? null,
        brand: brand ?? null,
        includeReconciled,
        limit: params.limit,
      },
      ranges: { start, end },
      counts,
      interdata,
      acquirers: {
        cielo,
        sipag,
        sicredi,
      },
      groups: Array.from(groupsMap.values()),
    };
  }

  async listPending(params: {
    source: 'INTERDATA' | 'CIELO' | 'SIPAG' | 'SICREDI';
    date?: string;
    sortBy?: 'saleDatetime' | 'grossAmount';
    sortDir?: 'asc' | 'desc';
  }) {
    await this.cleanupCancelledReconciliations();
    const sortBy = params.sortBy ?? 'saleDatetime';
    const sortDir = params.sortDir ?? 'asc';

    return this.repository.listPendingBySourcePaged({
      source: params.source,
      date: params.date,
      sortBy,
      sortDir,
    });
  }

  async listPendingDuplicateGroups(params: {
    source: 'INTERDATA' | 'CIELO' | 'SIPAG' | 'SICREDI';
    date?: string;
    verbose?: boolean;
  }): Promise<DuplicateGroup[]> {
    const groups = await this.repository.listDuplicateGroups({
      source: params.source,
      date: params.date,
    });
    if (params.verbose) {
      this.logger.log(
        `Pending duplicates: source=${params.source} date=${params.date ?? 'N/A'} groups=${groups.length}`,
      );
    }
    return groups;
  }

  async auditDuplicates(params: {
    acquirer: 'CIELO' | 'SIPAG' | 'SICREDI' | 'ALL';
    from: string;
    to: string;
    onlySuspicious: boolean;
  }): Promise<AuditDuplicateRow[]> {
    const rows = await this.repository.auditDuplicates({
      acquirer: params.acquirer,
      from: params.from,
      to: params.to,
    });
    const toDebugText = (value: unknown) => {
      if (Buffer.isBuffer(value)) {
        return value.toString('utf8');
      }
      return String(value);
    };

    const parseIds = (value: unknown): number[] => {
      if (value === null || typeof value === 'undefined') {
        return [];
      }
      if (typeof value === 'number') {
        return [value];
      }
      if (Array.isArray(value)) {
        return value
          .map((entry) => Number(entry))
          .filter((entry) => Number.isFinite(entry));
      }
      const text = Buffer.isBuffer(value) ? value.toString('utf8') : String(value);
      const tokens = text.split(/[;,]/);
      return tokens
        .map((entry) => entry.trim())
        .filter(Boolean)
        .map((entry) => Number(entry))
        .filter((entry) => Number.isFinite(entry));
    };

    const mapped = rows.map((row: AuditDuplicateRowRaw) => {
      const erpCount = Number(row.ERP_COUNT ?? 0);
      const acqCount = Number(row.ACQ_COUNT ?? 0);
      const reconCount = Number(row.RECON_COUNT ?? 0);
      const erpDup = erpCount > 1;
      const acqDup = acqCount > 1;
      const reconDup = reconCount > 1;
      const mismatch =
        erpCount !== acqCount || (reconCount > 0 && (erpCount === 0 || acqCount === 0));
      const erpIds = parseIds(row.ERP_IDS);
      const acqIds = parseIds(row.ACQ_IDS);
      const reconIds = parseIds(row.RECON_IDS);
      if (process.env.NODE_ENV === 'development') {
        this.logger.debug(
          `Audit IDs raw: ERP=${toDebugText(row.ERP_IDS ?? '')} ACQ=${toDebugText(row.ACQ_IDS ?? '')} RECON=${toDebugText(row.RECON_IDS ?? '')}`,
        );
        this.logger.debug(
          `Audit IDs parsed: ERP=${JSON.stringify(erpIds)} ACQ=${JSON.stringify(acqIds)} RECON=${JSON.stringify(reconIds)}`,
        );
      }

      return {
        key: {
          saleDate: row.SALE_DATE ?? null,
          methodGroup: row.METHOD_GROUP ?? null,
          grossAmount:
            row.GROSS_AMOUNT === null || typeof row.GROSS_AMOUNT === 'undefined'
              ? null
              : Number(row.GROSS_AMOUNT),
        },
        erpCount,
        acqCount,
        reconCount,
        erpIds,
        acqIds,
        reconIds,
        flags: {
          erpDup,
          acqDup,
          reconDup,
          mismatch,
        },
      };
    });

    if (!params.onlySuspicious) {
      return mapped;
    }
    return mapped.filter(
      (row) =>
        row.flags.erpDup ||
        row.flags.acqDup ||
        row.flags.reconDup ||
        row.flags.mismatch,
    );
  }

  private buildOrderBy(
    sortBy: 'datetime' | 'amount' | undefined,
    sortDir: 'asc' | 'desc' | undefined,
    columns: { datetime: string; amount: string },
  ) {
    if (!sortBy) {
      return `${columns.datetime} DESC, ID DESC`;
    }
    if (sortBy === 'amount') {
      return `${columns.amount} ${sortDir === 'desc' ? 'DESC' : 'ASC'}, ID DESC`;
    }
    return `${columns.datetime} ${sortDir === 'asc' ? 'ASC' : 'DESC'}, ID DESC`;
  }

  private safeJson(payload: unknown): string | null {
    try {
      return JSON.stringify(payload, (_key, value) => {
        if (value instanceof Date) {
          return toFbTimestampString(value);
        }
        return value;
      });
    } catch {
      return null;
    }
  }

  private async upsertReconStatus(input: {
    interdataId: number;
    runId: number;
    reason: string;
    details?: Record<string, unknown> | null;
  }) {
    const existing = await this.dbService.query<{ INTERDATA_ID: number }>(
      'SELECT FIRST 1 INTERDATA_ID FROM T_RECON_STATUS WHERE INTERDATA_ID = ?',
      [input.interdataId],
    );
    const detailsJson = this.safeJson(input.details ?? null);
    if (existing.length) {
      await this.dbService.execute(
        'UPDATE T_RECON_STATUS SET LAST_RUN_ID = ?, LAST_ATTEMPT_AT = CURRENT_TIMESTAMP, LAST_REASON = ?, LAST_DETAILS = ?, ATTEMPTS = COALESCE(ATTEMPTS, 0) + 1 WHERE INTERDATA_ID = ?',
        [input.runId, input.reason, detailsJson, input.interdataId],
      );
      return;
    }
    await this.dbService.execute(
      'INSERT INTO T_RECON_STATUS (INTERDATA_ID, LAST_RUN_ID, LAST_ATTEMPT_AT, LAST_REASON, LAST_DETAILS, ATTEMPTS) VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?)',
      [input.interdataId, input.runId, input.reason, detailsJson, 1],
    );
  }

  private reconResultsColumnsCache?: Set<string>;

  private async getReconResultsColumns(): Promise<Set<string>> {
    if (this.reconResultsColumnsCache) {
      return this.reconResultsColumnsCache;
    }
    const rows = await this.dbService.query<{ FIELD_NAME: string }>(
      "SELECT TRIM(rf.RDB$FIELD_NAME) as FIELD_NAME FROM RDB$RELATION_FIELDS rf WHERE rf.RDB$RELATION_NAME = ?",
      ['RECON_RESULTS'],
    );
    const set = new Set<string>(rows.map((row) => String(row.FIELD_NAME).trim().toUpperCase()));
    this.reconResultsColumnsCache = set;
    return set;
  }

  private async storeReconResult(runId: number, payload: Record<string, unknown>) {
    const columns = await this.getReconResultsColumns();
    if (!columns.size) {
      this.logger.warn('RECON_RESULTS sem colunas conhecidas; resultado dryRun nao persistido');
      return;
    }
    const json = this.safeJson(payload);
    const payloadParams = (payload as { params?: Record<string, unknown> }).params;
    const rawDate = typeof payloadParams?.date === 'string' ? payloadParams.date : '';
    const refDate =
      rawDate && /^\d{4}-\d{2}-\d{2}$/.test(rawDate) ? rawDate : new Date();
    const entries: Array<[string, unknown]> = [
      ['RUN_ID', runId],
      ['CREATED_AT', new Date()],
      ['REF_DATE', refDate],
      ['RESULT_JSON', json],
      ['DATA_JSON', json],
      ['PAYLOAD', json],
      ['SUMMARY', json],
      ['META_JSON', json],
    ];
    const filtered = entries.filter(([column]) => columns.has(column));
    if (columns.has('REF_DATE')) {
      this.logger.debug(
        `RECON_RESULTS: REF_DATE preenchido (${rawDate || 'CURRENT_DATE'}) para runId=${runId}`,
      );
    }
    if (!filtered.length) {
      this.logger.warn('RECON_RESULTS sem coluna JSON; resultado dryRun nao persistido');
      return;
    }
    const cols = filtered.map(([column]) => column);
    const vals = filtered.map(([, value]) => value ?? null);
    const sql =
      `INSERT INTO RECON_RESULTS (${cols.join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`;
    await this.dbService.execute(sql, vals);
  }

  private async runProbe(
    interdataId: number,
    acquirers: ('CIELO' | 'SIPAG' | 'SICREDI')[],
    verbose: boolean,
  ) {
    const sale = await this.dbService.transaction(async (tx) => {
      return (await this.repository.getInterdataSale(tx, interdataId)) as InterdataSale | null;
    });
    if (!sale) {
      throw new NotFoundException('interdata_nao_encontrado');
    }
    const result = await this.matchCanonExclusive(sale, acquirers, verbose);
    return {
      ok: true,
      data: {
        probe: true,
        interdataId,
        result,
      },
    };
  }

  async reconcileManual(dto: ManualReconciliationDto) {
    await this.cleanupCancelledReconciliations();
    const provider = dto.acquirerProvider?.toUpperCase() as ManualReconciliationProvider;
    if (provider === 'CIELO_EDI') {
      throw new BadRequestException('CIELO_EDI nao suportado ainda');
    }
    if (provider !== 'CIELO' && provider !== 'SIPAG') {
      throw new BadRequestException('acquirerProvider invalido');
    }
    const runId = Date.now();

    const result = await this.dbService.transaction(async (tx) => {
      const interdata = (await this.repository.getInterdataSale(tx, dto.interdataId)) as InterdataSale | null;
      if (!interdata) {
        throw new ConflictException('interdata_nao_encontrado');
      }

      const acquirer =
        provider === 'CIELO'
          ? ((await this.repository.getCieloSale(tx, dto.acquirerSaleId)) as CieloSale | null)
          : ((await this.repository.getSipagSale(tx, dto.acquirerSaleId)) as SipagSale | null);
      if (!acquirer) {
        throw new ConflictException('acquirer_nao_encontrado');
      }

      const acquirerResolvedStatus =
        provider === 'CIELO'
          ? (acquirer as CieloSale).STATUS ?? null
          : (acquirer as SipagSale).STATUS ?? null;
      if (isCancelledStatus(acquirerResolvedStatus)) {
        throw new ConflictException('acquirer_status_cancelado');
      }

      const interdataAlready = await this.repository.existsReconciliationByInterdata(
        tx,
        dto.interdataId,
      );
      if (interdataAlready) {
        throw new ConflictException('interdata_ja_conciliado');
      }

      const acquirerAlready = await this.repository.existsReconciliationByAcquirer(
        tx,
        provider,
        dto.acquirerSaleId,
      );
      if (acquirerAlready) {
        throw new ConflictException('acquirer_ja_conciliado');
      }

      const acqProvider = provider.toLowerCase();
      const acqAuthCode =
        provider === 'CIELO'
          ? (acquirer as CieloSale).AUTH_CODE ?? null
          : (acquirer as SipagSale).AUTH_NO ?? null;
      const acqNsu =
        provider === 'CIELO'
          ? (acquirer as CieloSale).NSU_DOC ?? null
          : (acquirer as SipagSale).TRANSACTION_NO ?? null;
      const saleDatetime = interdata.SALE_DATETIME ?? null;
      const acqDatetime = acquirer.SALE_DATETIME ?? null;
      const gross = interdata.GROSS_AMOUNT ?? null;
      const acqGross = acquirer.GROSS_AMOUNT ?? null;
      const net = interdata.NET_AMOUNT ?? null;
      const acqNet = acquirer.NET_AMOUNT ?? null;

      const payload = [
        interdata.ID,
        dto.acquirerSaleId,
        provider,
        acqProvider,
        interdata.SALE_NO ?? interdata.NSU ?? null,
        interdata.AUTH_NSU ?? interdata.NSU ?? null,
        acqAuthCode,
        acqNsu,
        saleDatetime,
        acqDatetime,
        gross,
        acqGross,
        net,
        acqNet,
        'MANUAL',
        'MANUAL',
        100,
        amountDiff(acqGross, gross),
        new Date(),
      ];

      const extras: Record<string, unknown> = {
        PAYMENT_METHOD: interdata.PAYMENT_METHOD ?? (acquirer as any).PAYMENT_METHOD ?? null,
        PAYMENT_TYPE: interdata.PAYMENT_TYPE ?? (acquirer as any).MODALITY ?? null,
        CARD_MODE: interdata.CARD_MODE ?? (acquirer as any).CREDIT_DEBIT_IND ?? null,
        CARD_BRAND_RAW: interdata.CARD_BRAND_RAW ?? null,
        BRAND: interdata.CARD_BRAND_RAW ?? interdata.BRAND ?? (acquirer as any).BRAND ?? null,
        ACQ_PAYMENT_METHOD: (acquirer as any).PAYMENT_METHOD ?? null,
        ACQ_BRAND: (acquirer as any).BRAND ?? null,
        ACQ_FEE_AMOUNT:
          typeof (acquirer as any).CANON_FEE_AMOUNT === 'number'
            ? (acquirer as any).CANON_FEE_AMOUNT
            : typeof (acquirer as any).FEE_AMOUNT === 'number'
              ? (acquirer as any).FEE_AMOUNT
              : typeof (acquirer as any).MDR_AMOUNT === 'number'
                ? (acquirer as any).MDR_AMOUNT
                : null,
        ACQ_PERC_TAXA:
          typeof (acquirer as any).CANON_PERC_TAXA === 'number'
            ? (acquirer as any).CANON_PERC_TAXA
            : null,
        RECON_RULE_APPLIED: 'manual',
        IS_ACTIVE: 1,
        RUN_ID: runId,
        CREATED_BY: 'MANUAL',
        SOURCE_RUN_KIND: 'MANUAL',
      };

      await this.repository.insertReconciliation(tx, payload, {
        source: 'MANUAL',
        reason: dto.reason,
        notes: dto.notes,
      }, extras);

      const reconciliationId = await this.repository.getReconciliationId(
        tx,
        dto.interdataId,
        provider,
        dto.acquirerSaleId,
      );
      if (!reconciliationId) {
        throw new Error('reconciliation_id_missing');
      }

      await this.matchRepository.insertMatch(tx, {
        reconciliationId,
        interdataSaleId: dto.interdataId,
        acqProvider: provider,
        acqSaleId: dto.acquirerSaleId,
        matchRule: 'MANUAL',
        matchMeta: this.buildManualMatchMeta(interdata, acquirer),
        runId,
        erpSnapshot: this.safeJson({ ...interdata }),
        acqSnapshot: this.safeJson({ ...(acquirer as any) }),
        metaJson: this.safeJson({ reason: dto.reason, notes: dto.notes ?? null }),
      });

      const createdId = await this.repository.getReconciliationId(
        tx,
        interdata.ID,
        provider,
        dto.acquirerSaleId,
      );

      return { createdId };
    });

    this.logger.log(
      `Conciliação MANUAL criada: interdata=${dto.interdataId} acq=${provider}:${dto.acquirerSaleId} reason=${dto.reason}`,
    );

    return {
      ok: true,
      createdId: result.createdId,
      summary: {
        interdataId: dto.interdataId,
        acquirerProvider: provider,
        acquirerSaleId: dto.acquirerSaleId,
      },
    };
  }

  private async cleanupCancelledReconciliations(force = false): Promise<{ total: number }> {
    const now = Date.now();
    if (!force && this.cancelledCleanupInFlight) {
      return this.cancelledCleanupInFlight;
    }
    if (!force && now - this.cancelledCleanupLastAt < 60_000) {
      return { total: 0 };
    }
    this.cancelledCleanupInFlight = this.dbService.transaction(async (tx) => {
      const result = await this.repository.deactivateCancelledReconciliations(tx);
      if (result.total > 0) {
        this.logger.warn(
          `Conciliacoes desativadas por status cancelado: total=${result.total} cielo=${result.cielo} sipag=${result.sipag} sicredi=${result.sicredi}`,
        );
      }
      return { total: result.total };
    });
    try {
      const result = await this.cancelledCleanupInFlight;
      this.cancelledCleanupLastAt = Date.now();
      return result;
    } finally {
      this.cancelledCleanupInFlight = null;
    }
  }

  private async matchCascade(
    sale: InterdataSale,
    acquirers: ('CIELO' | 'SIPAG' | 'SICREDI')[],
    verbose: boolean,
    pixStats: {
      pixMatchedNsu: () => void;
      pixMatchedDatetimeAmount: () => void;
      pixSkippedNoCandidates: () => void;
      pixSkippedAmbiguous: () => void;
      pixSkippedPaymentMismatch: () => void;
      canonMatchedL1: () => void;
      canonMatchedL2: () => void;
      canonMatchedPixNoTerminal: () => void;
      canonMatchedL3: () => void;
      canonMatchedL4: () => void;
    },
  ): Promise<{ match: MatchResult | null; reason?: string; details?: Record<string, unknown> }> {
    return this.matchCanonExclusive(sale, acquirers, verbose);
  }

  private async matchByIdentifiers(
    sale: InterdataSale,
    identifiers: string[],
    acquirers: ('CIELO' | 'SIPAG' | 'SICREDI')[],
    verbose: boolean,
  ): Promise<MatchResult | null> {
    const interHasTime = hasTimeComponent(sale.SALE_DATETIME ?? null);
    const matches: Array<{
      acquirer: 'CIELO' | 'SIPAG' | 'SICREDI';
      candidate: CieloSale | SipagSale | SicrediSale;
    }> = [];

    if (acquirers.includes('CIELO')) {
      const cieloCandidates = (await this.repository.listCieloByIdentifiers(
        identifiers,
      )) as CieloSale[];
      for (const candidate of cieloCandidates) {
        if (isCancelledStatus(candidate.STATUS)) {
          continue;
        }
        matches.push({ acquirer: 'CIELO', candidate });
      }
    }

    if (acquirers.includes('SIPAG')) {
      const sipagCandidates = (await this.repository.listSipagByIdentifiers(
        identifiers,
      )) as SipagSale[];
      for (const candidate of sipagCandidates) {
        if (isCancelledStatus(candidate.STATUS)) {
          continue;
        }
        matches.push({ acquirer: 'SIPAG', candidate });
      }
    }

    const scored: CandidateEntry[] = [];
    for (const entry of matches) {
      if (!isWithinAmountTolerance(entry.candidate.GROSS_AMOUNT, sale.GROSS_AMOUNT)) {
        if (verbose) {
          this.logger.log(
            `Match NSU skip: interdata=${sale.ID} acq=${entry.acquirer}:${entry.candidate.ID} acqDt=${this.formatTimestamp(entry.candidate.SALE_DATETIME)} motivo=valor`,
          );
        }
        continue;
      }
      if (interHasTime && !isWithinTimeTolerance(sale.SALE_DATETIME, entry.candidate.SALE_DATETIME)) {
        if (verbose) {
          this.logger.log(
            `Match NSU skip: interdata=${sale.ID} acq=${entry.acquirer}:${entry.candidate.ID} acqDt=${this.formatTimestamp(entry.candidate.SALE_DATETIME)} motivo=tempo`,
          );
        }
        continue;
      }
      const diffMs = interHasTime
        ? timeDiffMs(sale.SALE_DATETIME, entry.candidate.SALE_DATETIME)
        : 0;
      if (diffMs === null) {
        continue;
      }
      const paymentMatch = this.resolvePaymentMatch(sale, entry.candidate, verbose);
      if (!paymentMatch.allowed) {
        continue;
      }
      scored.push({
        acquirer: entry.acquirer,
        candidate: entry.candidate,
        score: 100 + paymentMatch.scoreDelta,
        timeDiffMs: diffMs,
        amountDiffAbs: Math.abs(amountDiff(entry.candidate.GROSS_AMOUNT, sale.GROSS_AMOUNT)),
      });
    }

    if (verbose) {
      this.logger.log(
        `Match NSU: interdata=${sale.ID} interHasTime=${interHasTime} candidatos=${scored.length}`,
      );
    }

    const selection = this.selectBestCandidate(sale, scored, 'NSU', verbose, 'NSU');
    return selection.match;
  }

  private async matchCanonExclusive(
    sale: InterdataSale,
    acquirers: ('CIELO' | 'SIPAG' | 'SICREDI')[],
    verbose: boolean,
  ): Promise<{ match: MatchResult | null; reason?: string; details?: Record<string, unknown> }> {
    const rawCanonDate = sale.CANON_SALE_DATE ?? null;
    const methodGroup = sale.CANON_METHOD_GROUP?.trim().toUpperCase() ?? null;
    const grossAmount =
      typeof sale.CANON_GROSS_AMOUNT === 'number' ? sale.CANON_GROSS_AMOUNT : null;
    const saleDatetime = sale.SALE_DATETIME ?? (sale as { CREATED_AT?: Date }).CREATED_AT ?? null;
    const formatCanonDate = (value: unknown, fallback: Date | null): string | null => {
      if (value instanceof Date) {
        return toFbDateString(value);
      }
      const text = typeof value === 'string' ? value.trim() : '';
      if (text) {
        if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
          return text;
        }
        if (text.includes('T')) {
          const parsed = new Date(text);
          if (!Number.isNaN(parsed.getTime())) {
            return toFbDateString(parsed);
          }
        }
      }
      return fallback ? toFbDateString(fallback) : null;
    };
    const canonDate = formatCanonDate(rawCanonDate, saleDatetime);
    const toValidDate = (value: unknown): Date | null => {
      if (value instanceof Date && !Number.isNaN(value.getTime())) {
        return value;
      }
      if (typeof value === 'string' || typeof value === 'number') {
        const parsed = new Date(value);
        if (!Number.isNaN(parsed.getTime())) {
          return parsed;
        }
      }
      return null;
    };
    const interTs = toValidDate(saleDatetime);
    const interTerminal = canonTerminal(sale.CANON_TERMINAL_NO ?? null).digits;

    if (verbose) {
      this.logger.log(
        `Canon base: interdata=${sale.ID} date=${canonDate ?? 'N/A'} method=${methodGroup ?? 'N/A'} amount=${grossAmount ?? 'N/A'} terminal=${interTerminal || 'N/A'}`,
      );
    }

    if (!canonDate || !methodGroup || grossAmount === null || !saleDatetime || !interTs) {
      return {
        match: null,
        reason: 'canon_base_incompleta',
        details: { canonDate, methodGroup, grossAmount, saleDatetime: sale.SALE_DATETIME ?? null },
      };
    }

    const baseWindowMinutes = methodGroup === 'PIX' ? 12 * 60 : 3 * 60;
    const hasTime = hasTimeComponent(interTs);
    const baseWindow = hasTime
      ? {
          start: new Date(interTs.getTime() - baseWindowMinutes * 60 * 1000),
          end: new Date(interTs.getTime() + baseWindowMinutes * 60 * 1000),
        }
      : buildDayWindow(interTs);
    const baseStart = toFbTimestampString(baseWindow.start);
    const baseEnd = toFbTimestampString(baseWindow.end);

    const listByCanon = async <T>(
      table: string,
      acquirer: 'CIELO' | 'SIPAG' | 'SICREDI',
      include: boolean,
    ): Promise<T[]> => {
      if (!include) {
        return [];
      }
      if (verbose) {
        this.logger.log(`Canon consulta ${acquirer}: tabela=${table}`);
      }
      const sql =
        'SELECT c.ID, c.SALE_DATETIME, c.STATUS, c.GROSS_AMOUNT, c.NET_AMOUNT, ' +
        'c.CANON_SALE_DATE, c.CANON_METHOD_GROUP, c.CANON_METHOD, c.CANON_GROSS_AMOUNT, ' +
        'c.CANON_TERMINAL_NO, c.CANON_AUTH_CODE, c.CANON_NSU, c.CANON_INSTALLMENT_NO, ' +
        `c.CANON_INSTALLMENT_TOTAL, c.CANON_BRAND FROM ${table} c ` +
        'WHERE c.CANON_SALE_DATE = ? AND c.CANON_METHOD_GROUP = ? ' +
        'AND ABS(c.CANON_GROSS_AMOUNT - ?) <= 0.01 ' +
        'AND c.SALE_DATETIME BETWEEN ? AND ? ' +
        `AND NOT EXISTS (SELECT 1 FROM T_RECONCILIATION r WHERE r.ACQUIRER = '${acquirer}' AND r.ACQUIRER_ID = c.ID)`;
      const params = [canonDate, methodGroup, grossAmount, baseStart, baseEnd];
      if (verbose) {
        this.logger.log(`Canon SQL: ${sql} params=${JSON.stringify(params)}`);
      }
      return this.dbService.query<T>(sql, params);
    };

    const cieloCandidates = (await listByCanon<CieloSale>('V_PENDING_CIELO', 'CIELO', acquirers.includes('CIELO')))
      .filter((candidate) => !isCancelledStatus(candidate.STATUS));
    const sipagCandidates = (await listByCanon<SipagSale>('V_PENDING_SIPAG', 'SIPAG', acquirers.includes('SIPAG')))
      .filter((candidate) => !isCancelledStatus(candidate.STATUS));
    const sicrediCandidates = (await listByCanon<SicrediSale>('V_PENDING_SICREDI', 'SICREDI', acquirers.includes('SICREDI')))
      .filter((candidate) => !isCancelledStatus(candidate.STATUS));

    if (verbose) {
      this.logger.log(
        `Canon candidatos base: interdata=${sale.ID} cielo=${cieloCandidates.length} sipag=${sipagCandidates.length} sicredi=${sicrediCandidates.length} janela=${baseStart}..${baseEnd}`,
      );
    }

    const baseCandidates: CandidateEntry[] = [
      ...cieloCandidates.map((candidate) => ({ acquirer: 'CIELO' as const, candidate, score: 0, timeDiffMs: 0, amountDiffAbs: 0 })),
      ...sipagCandidates.map((candidate) => ({ acquirer: 'SIPAG' as const, candidate, score: 0, timeDiffMs: 0, amountDiffAbs: 0 })),
      ...sicrediCandidates.map((candidate) => ({ acquirer: 'SICREDI' as const, candidate, score: 0, timeDiffMs: 0, amountDiffAbs: 0 })),
    ];

    if (!baseCandidates.length) {
      return {
        match: null,
        reason: 'no_candidates',
        details: { saleId: sale.ID, canonDate, methodGroup, grossAmount, windowStart: baseStart, windowEnd: baseEnd },
      };
    }

    const getTimeParts = (date: Date) => {
      const hh = date.getHours();
      const mm = date.getMinutes();
      return {
        hh,
        mm,
        hhmm: `${String(hh).padStart(2, '0')}:${String(mm).padStart(2, '0')}`,
      };
    };
    const interTime = getTimeParts(interTs);

    type CandidateWithTime = CandidateEntry & {
      candTs: Date;
      deltaMin: number;
      hhmm: string;
      hh: number;
      mm: number;
      terminal: string | null;
      authCode: string | null;
      nsu: string | null;
    };

    const candidatesWithTime: CandidateWithTime[] = [];
    for (const entry of baseCandidates) {
      const candTs = toValidDate(entry.candidate.SALE_DATETIME);
      if (!candTs) {
        if (verbose) {
          this.logger.warn(
            `Canon candidato sem SALE_DATETIME valido: interdata=${sale.ID} acq=${entry.acquirer}:${entry.candidate?.ID}`,
          );
        }
        continue;
      }
      const deltaMin = Math.abs(diffMinutes(interTs, candTs));
      const candidateTerminal = canonTerminal((entry.candidate as any).CANON_TERMINAL_NO ?? null).digits;
      const authCode = normalizeIdentifier((entry.candidate as any).CANON_AUTH_CODE ?? (entry.candidate as any).AUTH_CODE ?? (entry.candidate as any).AUTH_NO);
      const nsu = normalizeIdentifier((entry.candidate as any).CANON_NSU ?? (entry.candidate as any).NSU_DOC ?? (entry.candidate as any).TRANSACTION_NO ?? (entry.candidate as any).NSU);
      const timeParts = getTimeParts(candTs);
      candidatesWithTime.push({
        ...entry,
        candTs,
        deltaMin,
        hhmm: timeParts.hhmm,
        hh: timeParts.hh,
        mm: timeParts.mm,
        terminal: candidateTerminal || null,
        authCode,
        nsu,
      });
    }

    if (!candidatesWithTime.length) {
      return {
        match: null,
        reason: 'no_candidates',
        details: {
          saleId: sale.ID,
          canonDate,
          methodGroup,
          grossAmount,
          windowStart: baseStart,
          windowEnd: baseEnd,
        },
      };
    }

    const debugCandidates = candidatesWithTime.slice(0, 10).map((entry) => ({
      acquirer: entry.acquirer,
      id: entry.candidate.ID,
      datetime: this.formatTimestamp(entry.candTs),
      hhmm: entry.hhmm,
      deltaMin: entry.deltaMin,
      terminal: entry.terminal,
      auth: entry.authCode,
      nsu: entry.nsu,
      amount: entry.candidate.GROSS_AMOUNT ?? null,
    }));

    if (methodGroup === 'PIX') {
      const pixToleranceCandidates = candidatesWithTime.filter(
        (entry) => entry.deltaMin <= PIX_TIME_TOLERANCE_MINUTES,
      );
      if (verbose) {
        this.logger.log(
          `Canon PIX tolerance: interdata=${sale.ID} toleranciaMin=${PIX_TIME_TOLERANCE_MINUTES} candidatos=${pixToleranceCandidates.length}`,
        );
      }
      if (pixToleranceCandidates.length === 1) {
        const best = pixToleranceCandidates[0];
        return {
          match: {
            ...this.buildMatchResult(sale, best.candidate, best.acquirer, 'CANON_PIX_TOLERANCE', 88, {
              candidateCount: candidatesWithTime.length,
              reconRuleApplied: 'auto_pix_tolerance',
              timeDiffMinutes: best.deltaMin,
            }),
            matchLayer: 2,
            matchConfidence: 88,
            matchReason: `PIX_TOLERANCE_${PIX_TIME_TOLERANCE_MINUTES}M`,
          },
          details: {
            reason: 'pix_tolerance_unique',
            toleranceMinutes: PIX_TIME_TOLERANCE_MINUTES,
            candidates: debugCandidates,
          },
        };
      }
    }

    if (candidatesWithTime.length === 1) {
      const best = candidatesWithTime[0];
      if (verbose) {
        this.logger.log(
          `Canon unique base: interdata=${sale.ID} acq=${best.acquirer}:${best.candidate.ID} deltaMin=${best.deltaMin}`,
        );
      }
      return {
        match: {
          ...this.buildMatchResult(sale, best.candidate, best.acquirer, 'DATE_AMOUNT_METHOD', 90, {
            candidateCount: candidatesWithTime.length,
            timeDiffMinutes: best.deltaMin,
          }),
          matchLayer: 1,
          matchConfidence: 90,
          matchReason: 'unique_base',
        },
        details: {
          reason: 'unique_base',
          candidates: debugCandidates,
        },
      };
    }

    const applyTieBreak = (entries: CandidateWithTime[]): CandidateWithTime[] => {
      let filtered = entries;
      if (interTerminal) {
        const terminalMatches = filtered.filter((entry) => entry.terminal && entry.terminal === interTerminal);
        if (terminalMatches.length === 1) {
          return terminalMatches;
        }
        if (terminalMatches.length > 0) {
          filtered = terminalMatches;
        }
      }
      const authMatches = filtered.filter((entry) => Boolean(entry.authCode));
      if (authMatches.length === 1) {
        return authMatches;
      }
      if (authMatches.length > 0) {
        filtered = authMatches;
      }
      const nsuMatches = filtered.filter((entry) => Boolean(entry.nsu));
      if (nsuMatches.length === 1) {
        return nsuMatches;
      }
      if (nsuMatches.length > 0) {
        filtered = nsuMatches;
      }
      return filtered;
    };

    const logAmbiguous = (label: string, entries: CandidateWithTime[]) => {
      if (!verbose) {
        return;
      }
      const top = entries
        .slice(0, 5)
        .map((entry) => ({
          acq: entry.acquirer,
          id: entry.candidate.ID,
          datetime: this.formatTimestamp(entry.candTs),
          hhmm: entry.hhmm,
          deltaMin: entry.deltaMin,
          terminal: entry.terminal ?? null,
          auth: entry.authCode ?? null,
          nsu: entry.nsu ?? null,
          amount: entry.candidate.GROSS_AMOUNT ?? null,
        }));
      this.logger.log(
        `Canon ${label} ambiguo: interdata=${sale.ID} total=${entries.length} top=${JSON.stringify(top)}`,
      );
    };

    const resolveLayer = (
      label: string,
      entries: CandidateWithTime[],
      reason: string,
      score: number,
      layer: number,
    ) => {
      if (!entries.length) {
        return null;
      }
      const reduced = applyTieBreak(entries);
      if (reduced.length === 1) {
        const best = reduced[0];
        if (verbose) {
          this.logger.log(
            `Canon ${label} match: interdata=${sale.ID} acq=${best.acquirer}:${best.candidate.ID} deltaMin=${best.deltaMin} terminalBase=${interTerminal || 'N/A'} terminalCand=${best.terminal || 'N/A'}`,
          );
        }
        return {
          match: {
            ...this.buildMatchResult(sale, best.candidate, best.acquirer, 'DATE_AMOUNT_METHOD', score, {
              candidateCount: entries.length,
              timeDiffMinutes: best.deltaMin,
            }),
            matchLayer: layer,
            matchConfidence: score,
            matchReason: reason,
          },
          details: {
            reason,
            candidates: debugCandidates,
          },
        };
      }
      logAmbiguous(label, reduced);
      return null;
    };

    const layer0 = candidatesWithTime.filter((entry) => entry.hhmm === interTime.hhmm);
    const resolved0 = resolveLayer('T0', layer0, 'T0_exact_hhmm', 85, 2);
    if (resolved0) {
      return resolved0;
    }

    const layer1 = candidatesWithTime.filter(
      (entry) =>
        entry.mm === interTime.mm &&
        (entry.hh === (interTime.hh + 1) % 24 || entry.hh === (interTime.hh + 23) % 24),
    );
    const resolved1 = resolveLayer('T1', layer1, 'T1_pm1h_same_minute', 80, 3);
    if (resolved1) {
      return resolved1;
    }

    const layer2 = candidatesWithTime.filter(
      (entry) =>
        entry.mm === interTime.mm &&
        (entry.hh === (interTime.hh + 2) % 24 || entry.hh === (interTime.hh + 22) % 24),
    );
    const resolved2 = resolveLayer('T2', layer2, 'T2_pm2h_same_minute', 75, 4);
    if (resolved2) {
      return resolved2;
    }

    const sortedByDelta = [...candidatesWithTime].sort((a, b) => a.deltaMin - b.deltaMin);
    if (sortedByDelta.length >= 2) {
      const best = sortedByDelta[0];
      const second = sortedByDelta[1];
      const limit = methodGroup === 'PIX' ? 180 : 60;
      if (best.deltaMin <= limit && best.deltaMin + 5 < second.deltaMin) {
        if (verbose) {
          this.logger.log(
            `Canon T3 match: interdata=${sale.ID} acq=${best.acquirer}:${best.candidate.ID} deltaMin=${best.deltaMin} gap=${second.deltaMin - best.deltaMin}`,
          );
        }
        return {
          match: {
            ...this.buildMatchResult(sale, best.candidate, best.acquirer, 'DATE_AMOUNT_METHOD', 70, {
              candidateCount: sortedByDelta.length,
              timeDiffMinutes: best.deltaMin,
            }),
            matchLayer: 5,
            matchConfidence: 70,
            matchReason: 'T3_closest_delta',
          },
          details: {
            reason: 'T3_closest_delta',
            candidates: debugCandidates,
          },
        };
      }
      logAmbiguous('T3', sortedByDelta);
    }

    return {
      match: null,
      reason: 'ambiguous_time',
      details: {
        saleId: sale.ID,
        canonDate,
        methodGroup,
        grossAmount,
        windowStart: baseStart,
        windowEnd: baseEnd,
        candidates: debugCandidates,
      },
    };
  }

  private async matchCanonLayers(
    sale: InterdataSale,
    verbose: boolean,
    stats: {
      canonMatchedL1: () => void;
      canonMatchedL2: () => void;
      canonMatchedPixNoTerminal: () => void;
      canonMatchedL3: () => void;
      canonMatchedL4: () => void;
    },
  ): Promise<{ match: MatchResult | null; reason?: string }> {
    const canonDate = sale.CANON_SALE_DATE ?? (sale.SALE_DATETIME ? toFbDateString(sale.SALE_DATETIME) : null);
    const methodGroup = sale.CANON_METHOD_GROUP ?? null;
    const grossAmount = typeof sale.CANON_GROSS_AMOUNT === 'number' ? sale.CANON_GROSS_AMOUNT : sale.GROSS_AMOUNT ?? null;
    if (!canonDate || !methodGroup || typeof grossAmount !== 'number') {
      return { match: null, reason: 'canon_missing_fields' };
    }

    const interdataId = sale.ID;
    const terminal = sale.CANON_TERMINAL_NO?.trim() || '';
    const hasTerminal = terminal.length > 0;

    const countInterdataByKey = async (extraSql: string, params: unknown[]) => {
      const rows = await this.dbService.query<{ TOTAL: number }>(
        `SELECT COUNT(*) AS TOTAL FROM V_PENDING_INTERDATA i WHERE i.CANON_SALE_DATE = ? AND i.CANON_METHOD_GROUP = ? AND i.CANON_GROSS_AMOUNT = ? ${extraSql}`,
        [canonDate, methodGroup, grossAmount, ...params],
      );
      return rows[0]?.TOTAL ?? 0;
    };

    const listCieloByKey = async (extraSql: string, params: unknown[]) => {
      return this.dbService.query<CieloSale>(
        `SELECT * FROM V_PENDING_CIELO c WHERE c.CANON_SALE_DATE = ? AND c.CANON_METHOD_GROUP = ? AND c.CANON_GROSS_AMOUNT = ? ${extraSql}`,
        [canonDate, methodGroup, grossAmount, ...params],
      );
    };

    // L1: date+method_group+amount+terminal (non-empty) unique 1:1
    if (hasTerminal) {
      const candidates = await listCieloByKey('AND c.CANON_TERMINAL_NO = ? AND TRIM(c.CANON_TERMINAL_NO) <> \'\'', [terminal]);
      const valid = candidates.filter((candidate) => !isCancelledStatus(candidate.STATUS));
      const interCount = await countInterdataByKey('AND i.CANON_TERMINAL_NO = ? AND TRIM(i.CANON_TERMINAL_NO) <> \'\'', [terminal]);
      if (valid.length === 1 && interCount === 1) {
        stats.canonMatchedL1();
        return {
          match: this.buildMatchResultWithLayer(
            sale,
            valid[0],
            'CANON_L1',
            1,
            95,
            'L1: date+method_group+amount+terminal',
          ),
        };
      }
      if (verbose && valid.length > 1) {
        this.logger.log(`Canon L1 ambiguo: interdata=${interdataId} candidatos=${valid.length}`);
      }
    }

    // L2: CARD date+amount unique when terminal missing; PIX date+amount with NSU present
    if (methodGroup === 'CARD' && !hasTerminal) {
      const candidates = await listCieloByKey('', []);
      const valid = candidates.filter((candidate) => !isCancelledStatus(candidate.STATUS));
      const interCount = await countInterdataByKey('', []);
      if (valid.length === 1 && interCount === 1) {
        stats.canonMatchedL2();
        return {
          match: this.buildMatchResultWithLayer(
            sale,
            valid[0],
            'CANON_L2',
            2,
            85,
            'L2: date+method_group+amount (unique) terminal missing',
          ),
        };
      }
    }
    if (methodGroup === 'PIX') {
      const enforceTerminal = hasTerminal && /^\d+$/.test(terminal);
      const extraSql = `AND c.CANON_NSU IS NOT NULL ${enforceTerminal ? 'AND c.CANON_TERMINAL_NO = ?' : ''}`;
      const candidates = await listCieloByKey(extraSql, enforceTerminal ? [terminal] : []);
      const valid = candidates.filter((candidate) => !isCancelledStatus(candidate.STATUS));
      const interCount = await countInterdataByKey(
        enforceTerminal ? 'AND i.CANON_TERMINAL_NO = ?' : '',
        enforceTerminal ? [terminal] : [],
      );
      if (valid.length === 1 && interCount === 1) {
        stats.canonMatchedL2();
        return {
          match: this.buildMatchResultWithLayer(
            sale,
            valid[0],
            'CANON_L2',
            2,
            85,
            'L2: date+method_group+amount (unique) PIX NSU present',
          ),
        };
      }
    }

    // PIX no terminal: date+amount unique 1:1
    if (methodGroup === 'PIX' && !hasTerminal) {
      const candidates = await listCieloByKey('', []);
      const valid = candidates.filter((candidate) => !isCancelledStatus(candidate.STATUS));
      const interCount = await countInterdataByKey(
        "AND (i.CANON_TERMINAL_NO IS NULL OR TRIM(i.CANON_TERMINAL_NO) = '')",
        [],
      );
      if (valid.length === 1 && interCount === 1) {
        if (verbose) {
          this.logger.log(
            `Canon PIX no terminal match: interdata=${interdataId} cielo=${valid[0].ID} date=${canonDate} amt=${grossAmount}`,
          );
        }
        stats.canonMatchedPixNoTerminal();
        return {
          match: this.buildMatchResultWithLayer(
            sale,
            valid[0],
            'CANON_L2',
            5,
            90,
            'PIX_NO_TERMINAL_DATE_AMOUNT',
          ),
        };
      }
      if (verbose && valid.length > 1) {
        this.logger.log(`Canon PIX no terminal ambiguo: interdata=${interdataId} candidatos=${valid.length}`);
      }
    }

    // L3: date+method_group+amount + time window <= 15m
    if (sale.SALE_DATETIME) {
      const timeWindowMinutes = methodGroup === 'PIX' ? PIX_TIME_TOLERANCE_MINUTES : 15;
      const candidates = await this.dbService.query<CieloSale>(
        `SELECT * FROM V_PENDING_CIELO c WHERE c.CANON_SALE_DATE = ? AND c.CANON_METHOD_GROUP = ? AND c.CANON_GROSS_AMOUNT = ? AND ABS(DATEDIFF(MINUTE, ?, c.SALE_DATETIME)) <= ?`,
        [canonDate, methodGroup, grossAmount, sale.SALE_DATETIME, timeWindowMinutes],
      );
      const valid = candidates.filter((candidate) => !isCancelledStatus(candidate.STATUS));
      const interCount = await this.dbService.query<{ TOTAL: number }>(
        `SELECT COUNT(*) AS TOTAL FROM V_PENDING_INTERDATA i WHERE i.CANON_SALE_DATE = ? AND i.CANON_METHOD_GROUP = ? AND i.CANON_GROSS_AMOUNT = ? AND ABS(DATEDIFF(MINUTE, ?, i.SALE_DATETIME)) <= ?`,
        [canonDate, methodGroup, grossAmount, sale.SALE_DATETIME, timeWindowMinutes],
      );
      const interTotal = interCount[0]?.TOTAL ?? 0;
      if (valid.length === 1 && interTotal === 1) {
        stats.canonMatchedL3();
        return {
          match: this.buildMatchResultWithLayer(
            sale,
            valid[0],
            'CANON_L3',
            3,
            75,
            `L3: date+method_group+amount + time_window<=${timeWindowMinutes}m (unique)`,
          ),
        };
      }
    }

    // L4: date+method_group+amount unique 1:1
    const candidatesL4 = await listCieloByKey('', []);
    const validL4 = candidatesL4.filter((candidate) => !isCancelledStatus(candidate.STATUS));
    const interCountL4 = await countInterdataByKey('', []);
    if (validL4.length === 1 && interCountL4 === 1) {
      stats.canonMatchedL4();
      return {
        match: this.buildMatchResultWithLayer(
          sale,
          validL4[0],
          'CANON_L4',
          4,
          65,
          'L4: date+method_group+amount unique 1:1',
        ),
      };
    }

    return { match: null, reason: 'canon_no_match' };
  }

  private async matchByDatetimeAmount(
    sale: InterdataSale,
    acquirers: ('CIELO' | 'SIPAG' | 'SICREDI')[],
    verbose: boolean,
  ): Promise<{ match: MatchResult | null; reason?: string }> {
    const interHasTime = hasTimeComponent(sale.SALE_DATETIME ?? null);
    const candidates: Array<{
      acquirer: 'CIELO' | 'SIPAG' | 'SICREDI';
      candidate: CieloSale | SipagSale | SicrediSale;
    }> = [];
    const window = interHasTime
      ? buildTimeWindow(sale.SALE_DATETIME!)
      : buildDayWindow(sale.SALE_DATETIME!);
    const { start, end } = window;
    const startTs = toFbTimestampString(start);
    const endTs = toFbTimestampString(end);

    if (acquirers.includes('CIELO')) {
      const list = (await this.repository.listCieloByDatetimeAmount({
        startTs,
        endTs,
        grossAmount: sale.GROSS_AMOUNT!,
        amountTolerance: AMOUNT_TOLERANCE,
      })) as CieloSale[];
      for (const candidate of list) {
        if (isCancelledStatus(candidate.STATUS)) {
          continue;
        }
        candidates.push({ acquirer: 'CIELO', candidate });
      }
    }

    if (acquirers.includes('SIPAG')) {
      const list = (await this.repository.listSipagByDatetimeAmount({
        startTs,
        endTs,
        grossAmount: sale.GROSS_AMOUNT!,
        amountTolerance: AMOUNT_TOLERANCE,
      })) as SipagSale[];
      for (const candidate of list) {
        if (isCancelledStatus(candidate.STATUS)) {
          continue;
        }
        candidates.push({ acquirer: 'SIPAG', candidate });
      }
    }

    if (verbose) {
      this.logger.log(
        `Match janela: interdata=${sale.ID} saleDt=${this.formatTimestamp(sale.SALE_DATETIME)} interHasTime=${interHasTime} window=${startTs}..${endTs} candidatos=${candidates.length}`,
      );
    }

    if (!candidates.length) {
      return { match: null, reason: 'sem candidatos na janela' };
    }

    const saleAmountKey = this.amountKey(sale.GROSS_AMOUNT);
    if (saleAmountKey) {
      const sipagCandidates = candidates.filter(
        (entry) => entry.acquirer === 'SIPAG' && this.amountKey(entry.candidate.GROSS_AMOUNT) === saleAmountKey,
      );
      if (sipagCandidates.length === 1) {
        const entry = sipagCandidates[0];
        const paymentMatch = this.resolvePaymentMatch(sale, entry.candidate, verbose);
        if (paymentMatch.allowed) {
          if (verbose) {
            this.logger.log(
              `Match unique amount: interdata=${sale.ID} amountKey=${saleAmountKey} acq=${entry.acquirer}:${entry.candidate.ID} motivo=UNIQUE_AMOUNT`,
            );
          }
          return {
            match: this.buildMatchResult(
              sale,
              entry.candidate,
              entry.acquirer,
              'DATETIME_AMOUNT_UNIQUE',
              100 + paymentMatch.scoreDelta,
              { candidateCount: candidates.length },
            ),
          };
        }
      }
    }

    const scored: CandidateEntry[] = [];
    for (const entry of candidates) {
      if (interHasTime && !isWithinTimeTolerance(sale.SALE_DATETIME, entry.candidate.SALE_DATETIME)) {
        if (verbose) {
          this.logger.log(
            `Match janela skip: interdata=${sale.ID} acq=${entry.acquirer}:${entry.candidate.ID} acqDt=${this.formatTimestamp(entry.candidate.SALE_DATETIME)} motivo=tempo`,
          );
        }
        continue;
      }
      if (!isWithinAmountTolerance(entry.candidate.GROSS_AMOUNT, sale.GROSS_AMOUNT)) {
        if (verbose) {
          this.logger.log(
            `Match janela skip: interdata=${sale.ID} acq=${entry.acquirer}:${entry.candidate.ID} acqDt=${this.formatTimestamp(entry.candidate.SALE_DATETIME)} motivo=valor`,
          );
        }
        continue;
      }
      const score = this.scoreStandardMatch(sale, entry.candidate, verbose, interHasTime);
      if (score >= 80) {
        const diffMs = interHasTime
          ? timeDiffMs(sale.SALE_DATETIME, entry.candidate.SALE_DATETIME)
          : 0;
        if (diffMs === null) {
          continue;
        }
        scored.push({
          acquirer: entry.acquirer,
          candidate: entry.candidate,
          score,
          timeDiffMs: diffMs,
          amountDiffAbs: Math.abs(amountDiff(entry.candidate.GROSS_AMOUNT, sale.GROSS_AMOUNT)),
        });
      }
    }

    const selection = this.selectBestCandidate(
      sale,
      scored,
      'DATETIME_AMOUNT',
      verbose,
      'DATETIME_AMOUNT',
    );

    return {
      match: selection.match,
      reason: selection.match ? undefined : selection.reason ?? 'score abaixo do limiar',
    };
  }

  private async matchByFallback(
    sale: InterdataSale,
    acquirers: ('CIELO' | 'SIPAG' | 'SICREDI')[],
    verbose: boolean,
  ): Promise<{ match: MatchResult | null; reason?: string }> {
    const saleDate = toFbDateString(sale.SALE_DATETIME!);
    const matches: Array<{
      acquirer: 'CIELO' | 'SIPAG' | 'SICREDI';
      candidates: (CieloSale | SipagSale | SicrediSale)[];
    }> = [];

    if (acquirers.includes('CIELO')) {
      const list = (await this.repository.listCieloByAmountDayBrand({
        saleDate,
        grossAmount: sale.GROSS_AMOUNT!,
        brand: undefined,
        amountTolerance: AMOUNT_TOLERANCE,
      })) as CieloSale[];
      matches.push({ acquirer: 'CIELO', candidates: list });
    }

    if (acquirers.includes('SIPAG')) {
      const list = (await this.repository.listSipagByAmountDayBrand({
        saleDate,
        grossAmount: sale.GROSS_AMOUNT!,
        brand: undefined,
        amountTolerance: AMOUNT_TOLERANCE,
      })) as SipagSale[];
      matches.push({ acquirer: 'SIPAG', candidates: list });
    }

    const scored: CandidateEntry[] = [];
    for (const entry of matches) {
      if (verbose && entry.candidates.length > 1) {
        this.logger.log(
          `Fallback candidatos: interdata=${sale.ID} acq=${entry.acquirer} total=${entry.candidates.length}`,
        );
      }
      for (const candidate of entry.candidates) {
        scored.push({
          acquirer: entry.acquirer,
          candidate,
          score: 0,
          timeDiffMs: 0,
          amountDiffAbs: Math.abs(amountDiff(candidate.GROSS_AMOUNT, sale.GROSS_AMOUNT)),
        });
      }
    }

    const selection = this.matchFallbackDateAmountMethod(sale, scored, verbose);
    return {
      match: selection.match,
      reason: selection.reason ?? 'fallback sem candidato unico',
    };
  }

  private matchFallbackDateAmountMethod(
    sale: InterdataSale,
    candidates: CandidateEntry[],
    verbose: boolean,
  ): { match: MatchResult | null; reason?: string } {
    const interMethod =
      sale.CANON_METHOD_GROUP ??
      normalizePaymentMethod(sale.PAYMENT_METHOD ?? sale.PAYMENT_TYPE ?? sale.CARD_MODE);
    if (!interMethod || interMethod === 'OTHER') {
      return { match: null, reason: 'metodo_interdata_indefinido' };
    }
    if (!sale.SALE_DATETIME) {
      return { match: null, reason: 'interdata_sem_data' };
    }

    const interBrand = normalizeBrand(sale.CARD_BRAND_RAW ?? sale.BRAND);
    const scored: CandidateEntry[] = [];

    for (const entry of candidates) {
      const candidate = entry.candidate;
      if (isCancelledStatus((candidate as any).STATUS)) {
        continue;
      }
      const candidateMethod =
        (candidate as any).CANON_METHOD_GROUP ??
        normalizePaymentMethod(
          (candidate as any).PAYMENT_METHOD ??
            (candidate as any).PAYMENT_TYPE ??
            (candidate as any).MODALITY ??
            (candidate as any).CREDIT_DEBIT_IND ??
            (candidate as any).CARD_MODE,
        );
      if (!candidateMethod || candidateMethod === 'OTHER') {
        continue;
      }
      if (interMethod !== candidateMethod) {
        continue;
      }
      if (!candidate.SALE_DATETIME) {
        continue;
      }
      const minutesDiff = Math.abs(diffMinutes(sale.SALE_DATETIME, candidate.SALE_DATETIME));
      if (minutesDiff > 120) {
        continue;
      }
      const amountDiffAbs = Math.abs(amountDiff(candidate.GROSS_AMOUNT, sale.GROSS_AMOUNT));
      if (amountDiffAbs > 0.01) {
        continue;
      }

      let score = 0;
      score += 50;
      score += 30;
      if (minutesDiff <= 10) {
        score += 20;
      } else if (minutesDiff <= 30) {
        score += 15;
      } else if (minutesDiff <= 60) {
        score += 10;
      } else {
        score += 5;
      }

      const acqBrand = normalizeBrand((candidate as any).BRAND);
      if (interBrand && acqBrand && interBrand !== acqBrand) {
        score -= 40;
      }

      if (verbose) {
        this.logger.log(
          `Fallback date+amount+method: interdata=${sale.ID} acq=${entry.acquirer}:${candidate.ID} diffMin=${minutesDiff} score=${score} brandInter=${interBrand ?? 'N/A'} brandAcq=${acqBrand ?? 'N/A'} pay=${interMethod} acqPay=${candidateMethod}`,
        );
      }

      scored.push({
        acquirer: entry.acquirer,
        candidate,
        score,
        timeDiffMs: minutesDiff * 60000,
        amountDiffAbs,
      });
    }

    if (!scored.length) {
      return { match: null, reason: 'sem candidatos validos' };
    }

    const sorted = [...scored].sort((a, b) => {
      if (b.score !== a.score) {
        return b.score - a.score;
      }
      if (a.timeDiffMs !== b.timeDiffMs) {
        return a.timeDiffMs - b.timeDiffMs;
      }
      return (a.candidate.ID ?? 0) - (b.candidate.ID ?? 0);
    });

    const best = sorted[0];
    const runnerUp = sorted[1];
    if (best.score < 75) {
      return { match: null, reason: 'score abaixo do limiar' };
    }
    if (runnerUp && best.score - runnerUp.score < 15) {
      return { match: null, reason: 'ambiguidade de score' };
    }

    const base = this.buildMatchResult(
      sale,
      best.candidate,
      best.acquirer,
      'DATE_AMOUNT_METHOD',
      best.score,
      {
        candidateCount: scored.length,
      },
    );
    return {
      match: {
        ...base,
        matchLayer: 2,
        matchConfidence: best.score,
        matchReason: 'DATE+AMOUNT+METHOD',
      },
    };
  }

  private async matchPix(
    sale: InterdataSale,
    verbose: boolean,
    stats: {
      pixMatchedNsu: () => void;
      pixMatchedDatetimeAmount: () => void;
      pixSkippedNoCandidates: () => void;
      pixSkippedAmbiguous: () => void;
      pixSkippedPaymentMismatch: () => void;
    },
  ): Promise<{ match: MatchResult | null; reason?: string }> {
    const amountCents = toCents(sale.GROSS_AMOUNT ?? null);
    const saleDatetime = sale.SALE_DATETIME ?? null;
    if (amountCents === null || !saleDatetime) {
      stats.pixSkippedNoCandidates();
      return { match: null, reason: 'pix_missing_base_fields' };
    }

    const nsuRaw = normalizeNsu(sale.AUTH_NSU ?? null);
    if (nsuRaw) {
      const nsuStripped = stripLeadingZeros(nsuRaw);
      const nsuValues = Array.from(new Set([nsuRaw, nsuStripped].filter((value) => value)));

      const windowStart = new Date(saleDatetime.getTime() - 180 * 60 * 1000);
      const windowEnd = new Date(saleDatetime.getTime() + 180 * 60 * 1000);
      const nsuCandidates = (await this.repository.listCieloByPixNsuAmount({
        nsus: nsuValues,
        grossAmount: sale.GROSS_AMOUNT ?? 0,
        startTs: toFbTimestampString(windowStart),
        endTs: toFbTimestampString(windowEnd),
      })) as CieloSale[];

      const nsuMatches = nsuCandidates.filter((candidate) => {
        const candidateNsu = normalizeNsu(
          (candidate as any).NSU_DOC ?? (candidate as any).E_NSU_DOC ?? null,
        );
        if (!candidateNsu) {
          return false;
        }
        const candidateStripped = stripLeadingZeros(candidateNsu);
        const nsuMatch = nsuValues.includes(candidateNsu) || nsuValues.includes(candidateStripped);
        const amountMatch = toCents(candidate.GROSS_AMOUNT) === amountCents;
        const diffMin = Math.abs(diffMinutes(candidate.SALE_DATETIME ?? saleDatetime, saleDatetime));
        return nsuMatch && amountMatch && diffMin <= 180;
      });

      if (verbose) {
        this.logger.log(
          `PIX NSU candidatos interdata=${sale.ID} nsu=${nsuRaw} total=${nsuCandidates.length} valid=${nsuMatches.length}`,
        );
      }

      if (nsuMatches.length === 1) {
        const only = nsuMatches[0];
        const diffMin = Math.abs(diffMinutes(only.SALE_DATETIME ?? saleDatetime, saleDatetime));
        stats.pixMatchedNsu();
        return {
          match: this.buildMatchResult(sale, only, 'CIELO', 'PIX_NSU_AMOUNT', 120, {
            candidateCount: nsuCandidates.length,
            timeDiffMinutes: diffMin,
          }),
        };
      }
      if (nsuMatches.length > 1) {
        stats.pixSkippedAmbiguous();
        return { match: null, reason: 'pix_ambiguous_nsu' };
      }
    }

    const dtStart = new Date(saleDatetime.getTime() - 120 * 60 * 1000);
    const dtEnd = new Date(saleDatetime.getTime() + 120 * 60 * 1000);
    const dtCandidates = (await this.repository.listCieloByPixAmountWindow({
      grossAmount: sale.GROSS_AMOUNT ?? 0,
      startTs: toFbTimestampString(dtStart),
      endTs: toFbTimestampString(dtEnd),
    })) as CieloSale[];

    const dtMatches = dtCandidates.filter((candidate) => {
      const diffMin = Math.abs(diffMinutes(candidate.SALE_DATETIME ?? saleDatetime, saleDatetime));
      if (diffMin > 120) {
        return false;
      }
      const amountMatch = toCents(candidate.GROSS_AMOUNT) === amountCents;
      if (!amountMatch) {
        return false;
      }
      const candidatePayment = normalizePaymentMethod((candidate as any).PAYMENT_METHOD ?? null);
      if (candidatePayment === 'CARD') {
        stats.pixSkippedPaymentMismatch();
        return false;
      }
      return true;
    });

    if (verbose) {
      const debug = dtMatches.map((candidate) => ({
        id: candidate.ID,
        diffMin: diffMinutes(candidate.SALE_DATETIME ?? saleDatetime, saleDatetime),
        amountDiff: amountDiff(candidate.GROSS_AMOUNT, sale.GROSS_AMOUNT),
        nsu: (candidate as any).NSU_DOC ?? (candidate as any).E_NSU_DOC ?? null,
      }));
      this.logger.log(
        `PIX datetime candidatos interdata=${sale.ID} total=${dtCandidates.length} valid=${dtMatches.length} details=${JSON.stringify(debug)}`,
      );
    }

    if (dtMatches.length === 1) {
      const only = dtMatches[0];
      const diffMin = Math.abs(diffMinutes(only.SALE_DATETIME ?? saleDatetime, saleDatetime));
      stats.pixMatchedDatetimeAmount();
      return {
        match: this.buildMatchResult(sale, only, 'CIELO', 'PIX_DATETIME_AMOUNT', 110, {
          candidateCount: dtCandidates.length,
          reconRuleApplied: 'auto_pix_tolerance',
          timeDiffMinutes: diffMin,
        }),
      };
    }
    if (dtMatches.length > 1) {
      stats.pixSkippedAmbiguous();
      return { match: null, reason: 'pix_ambiguous_amount_window' };
    }

    stats.pixSkippedNoCandidates();
    return { match: null, reason: 'pix_no_candidates' };
  }

  private scoreStandardMatch(
    sale: InterdataSale,
    candidate: CieloSale | SipagSale,
    verbose: boolean,
    interHasTime: boolean,
  ) {
    let score = 0;
    const diff = Math.abs(amountDiff(candidate.GROSS_AMOUNT, sale.GROSS_AMOUNT));
    if (diff <= AMOUNT_TOLERANCE) {
      score += 50;
    }
    if (isSameDay(sale.SALE_DATETIME, candidate.SALE_DATETIME)) {
      score += 30;
    }
    if (interHasTime) {
      const secondsDiff = diffSeconds(sale.SALE_DATETIME, candidate.SALE_DATETIME);
      if (secondsDiff !== null && secondsDiff <= 120) {
        score += 10;
      }
    }
    const paymentMatch = this.resolvePaymentMatch(sale, candidate, verbose);
    if (!paymentMatch.allowed) {
      return -1;
    }
    score += paymentMatch.scoreDelta;
    const brandMatch = normalizeBrand(sale.CARD_BRAND_RAW ?? sale.BRAND);
    const acqBrand = normalizeBrand(candidate.BRAND);
    if (brandMatch && acqBrand && brandMatch === acqBrand) {
      score += 5;
    }
    return score;
  }

  private scoreFallbackMatch(
    sale: InterdataSale,
    candidate: CieloSale | SipagSale,
    interHasTime: boolean,
  ) {
    let score = 0;
    const diff = Math.abs(amountDiff(candidate.GROSS_AMOUNT, sale.GROSS_AMOUNT));
    if (diff <= AMOUNT_TOLERANCE) {
      score += 50;
    }
    if (isSameDay(sale.SALE_DATETIME, candidate.SALE_DATETIME)) {
      score += 30;
    }
    if (!interHasTime) {
      score += 5;
    }
    const paymentMatch = this.resolvePaymentMatch(sale, candidate, false);
    if (!paymentMatch.allowed) {
      return -1;
    }
    score += paymentMatch.scoreDelta;
    const brandMatch = normalizeBrand(sale.CARD_BRAND_RAW ?? sale.BRAND);
    const acqBrand = normalizeBrand(candidate.BRAND);
    if (brandMatch && acqBrand && brandMatch === acqBrand) {
      score += 5;
    }
    return score;
  }

  private selectBestCandidate(
    sale: InterdataSale,
    candidates: CandidateEntry[],
    matchType: MatchType,
    verbose: boolean,
    context: string,
  ): { match: MatchResult | null; reason?: string } {
    if (!candidates.length) {
      return { match: null, reason: 'sem candidatos validos' };
    }
    const sorted = [...candidates].sort((a, b) => {
      if (a.timeDiffMs !== b.timeDiffMs) {
        return a.timeDiffMs - b.timeDiffMs;
      }
      if (a.amountDiffAbs !== b.amountDiffAbs) {
        return a.amountDiffAbs - b.amountDiffAbs;
      }
      return (b.candidate.ID ?? 0) - (a.candidate.ID ?? 0);
    });

    const best = sorted[0];
    const runnerUp = sorted[1];
    const timeTieThresholdMs = 60_000;
    const amountTieThreshold = 0.001;
    const scoreTieThreshold = 2;

    if (
      runnerUp &&
      (best.amountDiffAbs === 0 && runnerUp.amountDiffAbs > 0
        ? false
        : best.amountDiffAbs < runnerUp.amountDiffAbs &&
            runnerUp.amountDiffAbs - best.amountDiffAbs >= 0.01
          ? false
          : Math.abs(best.timeDiffMs - runnerUp.timeDiffMs) <= timeTieThresholdMs &&
            Math.abs(best.amountDiffAbs - runnerUp.amountDiffAbs) <= amountTieThreshold &&
            Math.abs(best.score - runnerUp.score) <= scoreTieThreshold)
    ) {
      if (verbose) {
        this.logger.log(
          `Match ${context} empate forte: interdata=${sale.ID} best=${best.acquirer}:${best.candidate.ID} runner=${runnerUp.acquirer}:${runnerUp.candidate.ID}`,
        );
      }
      return { match: null, reason: 'empate forte' };
    }

    if (verbose) {
      this.logger.log(
        `Match ${context} escolhido: interdata=${sale.ID} acq=${best.acquirer}:${best.candidate.ID} acqDt=${this.formatTimestamp(best.candidate.SALE_DATETIME)} diffMs=${best.timeDiffMs} diffValor=${best.amountDiffAbs} candidatos=${candidates.length}`,
      );
    }

    return {
      match: this.buildMatchResult(sale, best.candidate, best.acquirer, matchType, best.score, {
        candidateCount: candidates.length,
      }),
    };
  }

  private resolvePaymentMatch(
    sale: InterdataSale,
    candidate: CieloSale | SipagSale,
    verbose: boolean,
  ): {
    allowed: boolean;
    scoreDelta: number;
    inter: { paymentMethod: PaymentMethod; cardType: CardType };
    acq: { paymentMethod: PaymentMethod; cardType: CardType };
  } {
    const inter = normalizePaymentDetailsFromValues(
      sale.PAYMENT_METHOD,
      sale.PAYMENT_TYPE,
      sale.CARD_MODE,
    );
    const acq = normalizePaymentDetailsFromValues(
      (candidate as any).PAYMENT_METHOD,
      (candidate as any).MODALITY,
      (candidate as any).CREDIT_DEBIT_IND,
    );

    if (
      inter.paymentMethod !== 'UNKNOWN' &&
      acq.paymentMethod !== 'UNKNOWN' &&
      inter.paymentMethod !== acq.paymentMethod
    ) {
      if (verbose) {
        this.logger.log(
          `Tipo incompativel: inter=${this.formatPayment(inter)} acq=${this.formatPayment(acq)}`,
        );
      }
      return { allowed: false, scoreDelta: 0, inter, acq };
    }

    if (
      inter.paymentMethod === 'CARD' &&
      acq.paymentMethod === 'CARD' &&
      inter.cardType !== 'UNKNOWN' &&
      acq.cardType !== 'UNKNOWN' &&
      inter.cardType !== acq.cardType
    ) {
      if (verbose) {
        this.logger.log(
          `Tipo incompativel: inter=${this.formatPayment(inter)} acq=${this.formatPayment(acq)}`,
        );
      }
      return { allowed: false, scoreDelta: 0, inter, acq };
    }

    let scoreDelta = 0;
    if (inter.paymentMethod === 'UNKNOWN' || acq.paymentMethod === 'UNKNOWN') {
      scoreDelta -= 5;
      if (verbose) {
        this.logger.log(
          `Tipo indefinido: inter=${this.formatPayment(inter)} acq=${this.formatPayment(acq)}`,
        );
      }
    } else if (inter.paymentMethod === acq.paymentMethod) {
      scoreDelta += 10;
    }

    if (
      inter.paymentMethod === 'CARD' &&
      acq.paymentMethod === 'CARD' &&
      inter.cardType !== 'UNKNOWN' &&
      acq.cardType !== 'UNKNOWN' &&
      inter.cardType === acq.cardType
    ) {
      scoreDelta += 5;
    } else if (
      inter.paymentMethod === 'CARD' &&
      acq.paymentMethod === 'CARD' &&
      (inter.cardType === 'UNKNOWN' || acq.cardType === 'UNKNOWN')
    ) {
      scoreDelta -= 3;
    }

    return { allowed: true, scoreDelta, inter, acq };
  }

  private formatPayment(value: { paymentMethod: PaymentMethod; cardType: CardType }): string {
    if (value.paymentMethod !== 'CARD') {
      return value.paymentMethod;
    }
    return `${value.paymentMethod}/${value.cardType}`;
  }

  private amountKey(value?: number | null): string | null {
    if (typeof value !== 'number' || Number.isNaN(value)) {
      return null;
    }
    return value.toFixed(2);
  }

  private formatTimestamp(value?: Date | null): string {
    return value ? toFbTimestampString(value) : 'NULL';
  }

  private resolveReconRuleApplied(
    sale: InterdataSale,
    matchType: MatchType,
    timeDiffMinutes: number | null,
  ): ReconRuleApplied {
    const methodGroup = sale.CANON_METHOD_GROUP?.trim().toUpperCase() ?? null;
    const isPixRuleMatchType =
      matchType === 'PIX_DATETIME_AMOUNT' || matchType === 'CANON_PIX_TOLERANCE';
    if (methodGroup === 'PIX' && isPixRuleMatchType && timeDiffMinutes !== null && timeDiffMinutes <= PIX_TIME_TOLERANCE_MINUTES) {
      return 'auto_pix_tolerance';
    }
    return 'exact_match';
  }

  private buildMatchFlags(options: {
    timeDiffMinutes: number | null;
    amountDiffAbs: number;
    candidateCount: number;
    reconRuleApplied: ReconRuleApplied;
  }): MatchFlag[] {
    const flags = new Set<MatchFlag>();
    if (options.timeDiffMinutes !== null && options.timeDiffMinutes > 0) {
      flags.add('TIME_DIFF');
    }
    if (options.reconRuleApplied === 'auto_pix_tolerance') {
      flags.add('PIX_RULE_APPLIED');
    }
    if (options.candidateCount > 1) {
      flags.add('MULTI_MATCH');
    }
    if (options.amountDiffAbs > 0 && options.amountDiffAbs <= DEFAULT_AMOUNT_TOLERANCE) {
      flags.add('AMOUNT_DIFF_SMALL');
    }
    return Array.from(flags);
  }

  private buildMatchResult(
    sale: InterdataSale,
    candidate: CieloSale | SipagSale | SicrediSale,
    acquirer: 'CIELO' | 'SIPAG' | 'SICREDI',
    matchType: MatchType,
    score: number,
    options?: {
      candidateCount?: number;
      reconRuleApplied?: ReconRuleApplied;
      timeDiffMinutes?: number | null;
    },
  ): MatchResult {
    const acqAuthCode =
      acquirer === 'CIELO'
        ? (candidate as CieloSale).AUTH_CODE
        : acquirer === 'SIPAG'
          ? (candidate as SipagSale).AUTH_NO
          : (candidate as SicrediSale).AUTH_CODE;
    const acqNsu =
      acquirer === 'CIELO'
        ? (candidate as CieloSale).NSU_DOC
        : acquirer === 'SIPAG'
          ? (candidate as SipagSale).TRANSACTION_NO
          : (candidate as SicrediSale).NSU;
    const resolvedTimeDiffMinutes =
      typeof options?.timeDiffMinutes === 'number'
        ? options.timeDiffMinutes
        : sale.SALE_DATETIME && candidate.SALE_DATETIME
          ? Math.abs(diffMinutes(sale.SALE_DATETIME, candidate.SALE_DATETIME))
          : null;
    const resolvedCandidateCount = Math.max(1, options?.candidateCount ?? 1);
    const resolvedAmountDiff = amountDiff(candidate.GROSS_AMOUNT, sale.GROSS_AMOUNT);
    const resolvedRule =
      options?.reconRuleApplied ??
      this.resolveReconRuleApplied(sale, matchType, resolvedTimeDiffMinutes);
    const flags = this.buildMatchFlags({
      timeDiffMinutes: resolvedTimeDiffMinutes,
      amountDiffAbs: Math.abs(resolvedAmountDiff),
      candidateCount: resolvedCandidateCount,
      reconRuleApplied: resolvedRule,
    });

    return {
      acquirer,
      acquirerId: candidate.ID,
      candidate,
      acqAuthCode: acqAuthCode ?? null,
      acqNsu: acqNsu ?? null,
      acqSaleDatetime: candidate.SALE_DATETIME ?? null,
      acqGrossAmount: candidate.GROSS_AMOUNT ?? null,
      acqNetAmount: candidate.NET_AMOUNT ?? null,
      acqStatus: candidate.STATUS ?? null,
      matchType,
      matchScore: score,
      amountDiff: resolvedAmountDiff,
      timeDiffMinutes: resolvedTimeDiffMinutes,
      candidateCount: resolvedCandidateCount,
      reconRuleApplied: resolvedRule,
      flags,
    };
  }

  private buildMatchResultWithLayer(
    sale: InterdataSale,
    candidate: CieloSale,
    matchType: MatchType,
    layer: number,
    confidence: number,
    reason: string,
  ): MatchResult {
    const base = this.buildMatchResult(sale, candidate, 'CIELO', matchType, confidence);
    return {
      ...base,
      matchLayer: layer,
      matchConfidence: confidence,
      matchReason: reason,
    };
  }

  private getMatchLayerInfo(match: MatchResult): { layer: number; confidence: number; reason: string } | null {
    if (typeof match.matchLayer === 'number') {
      return {
        layer: match.matchLayer,
        confidence: match.matchConfidence ?? match.matchScore ?? 0,
        reason: match.matchReason ?? '',
      };
    }
    if (match.matchReason) {
      const fallback = this.getMatchLayerDefaults(match);
      return {
        layer: fallback.layer,
        confidence: match.matchConfidence ?? match.matchScore ?? fallback.confidence,
        reason: match.matchReason,
      };
    }
    return this.getMatchLayerDefaults(match);
  }

  private getMatchLayerDefaults(match: MatchResult): { layer: number; confidence: number; reason: string } {
    switch (match.matchType) {
      case 'NSU':
        return { layer: 1, confidence: 100, reason: 'NSU+AUTH+VALOR' };
      case 'PIX_NSU_AMOUNT':
        return { layer: 2, confidence: 90, reason: 'PIX_NSU+VALOR' };
      case 'DATETIME_AMOUNT_UNIQUE':
        return { layer: 4, confidence: 85, reason: 'VALOR+DATA±JANELA+UNICIDADE' };
      case 'DATETIME_AMOUNT':
        return { layer: 3, confidence: 75, reason: 'VALOR+DATA±JANELA' };
      case 'PIX_DATETIME_AMOUNT':
        return { layer: 3, confidence: 75, reason: 'PIX_VALOR+DATA±JANELA' };
      case 'CANON_PIX_TOLERANCE':
        return {
          layer: 2,
          confidence: 88,
          reason: `PIX_VALOR+DATA±${PIX_TIME_TOLERANCE_MINUTES}M`,
        };
      case 'FALLBACK_AMOUNT_DAY_BRAND':
        return { layer: 5, confidence: 50, reason: 'VALOR+DATA±1D+BANDEIRA' };
      case 'DATE_AMOUNT_METHOD':
        return { layer: 2, confidence: match.matchScore, reason: 'DATE+AMOUNT+METHOD' };
      case 'CANON_EXCLUSIVE_L1':
        return { layer: 1, confidence: 100, reason: 'CANON_LAYER1' };
      case 'CANON_EXCLUSIVE_L2':
        return { layer: 2, confidence: match.matchScore, reason: 'DATE+AMOUNT+METHOD' };
      case 'CANON_EXCLUSIVE_L0':
        return { layer: 0, confidence: match.matchScore, reason: 'DATE+AMOUNT+METHOD' };
      case 'CANON_FLEX_L5':
        return { layer: 5, confidence: match.matchScore, reason: 'DATE+AMOUNT+METHOD_FLEX' };
      default:
        return { layer: 0, confidence: match.matchScore ?? 0, reason: 'AUTO' };
    }
  }

  private buildIdentifierList(sale: InterdataSale): string[] {
    const values = [sale.NSU, sale.AUTH_NSU, sale.AUTH_CODE, sale.TID].filter(
      (value) => value !== null && value !== undefined,
    );
    const identifiers = new Set<string>();
    values.forEach((value) => {
      const normalized = normalizeIdentifier(value);
      if (normalized) {
        identifiers.add(normalized);
        if (normalized.startsWith('0')) {
          const stripped = stripLeadingZeros(normalized);
          if (stripped.length >= 4) {
            identifiers.add(stripped);
          }
        }
      }
    });
    return Array.from(identifiers);
  }

  private buildAutoMatchMeta(
    sale: InterdataSale,
    match: MatchResult,
    options?: { missingAcquirerId?: boolean },
  ): string | null {
    const datetimeDiffMinutes =
      sale.SALE_DATETIME && match.acqSaleDatetime
        ? Math.round((match.acqSaleDatetime.getTime() - sale.SALE_DATETIME.getTime()) / 60000)
        : null;
    const meta = {
      keysUsed: [match.matchType],
      flags: match.flags ?? [],
      reconRuleApplied: match.reconRuleApplied ?? 'exact_match',
      amountDiff: match.amountDiff ?? null,
      amountDiffAbs: Math.abs(match.amountDiff ?? 0),
      amountDiffSmall: match.flags?.includes('AMOUNT_DIFF_SMALL') ?? false,
      datetimeDiffMinutes,
      candidateCount: match.candidateCount ?? 1,
      nsu: match.acqNsu ?? sale.NSU ?? sale.AUTH_NSU ?? null,
      authCode: match.acqAuthCode ?? sale.AUTH_CODE ?? null,
      missingAcquirerId: Boolean(options?.missingAcquirerId),
    };
    return JSON.stringify(meta);
  }

  private buildManualMatchMeta(interdata: InterdataSale, acquirer: CieloSale | SipagSale) {
    const interDate = interdata.SALE_DATETIME ?? null;
    const acqDate = acquirer.SALE_DATETIME ?? null;
    const datetimeDiffMinutes =
      interDate && acqDate ? Math.round((acqDate.getTime() - interDate.getTime()) / 60000) : null;
    const meta = {
      keysUsed: ['MANUAL'],
      flags: [],
      reconRuleApplied: 'manual',
      amountDiff: amountDiff(acquirer.GROSS_AMOUNT, interdata.GROSS_AMOUNT),
      datetimeDiffMinutes,
      candidateCount: 1,
      nsu: interdata.AUTH_NSU ?? interdata.NSU ?? null,
      authCode:
        (acquirer as CieloSale).AUTH_CODE ??
        (acquirer as SipagSale).AUTH_NO ??
        null,
    };
    return JSON.stringify(meta);
  }

}

type MatchResult = {
  acquirer: 'CIELO' | 'SIPAG' | 'SICREDI';
  acquirerId: number;
  candidate?: CieloSale | SipagSale | SicrediSale;
  acqAuthCode?: string | null;
  acqNsu?: string | null;
  acqSaleDatetime?: Date | null;
  acqGrossAmount?: number | null;
  acqNetAmount?: number | null;
  acqStatus?: string | null;
  matchType: MatchType;
  matchScore: number;
  amountDiff: number;
  timeDiffMinutes?: number | null;
  candidateCount?: number;
  reconRuleApplied?: ReconRuleApplied;
  flags?: MatchFlag[];
  matchLayer?: number | null;
  matchConfidence?: number | null;
  matchReason?: string | null;
};

type MatchType =
  | 'NSU'
  | 'DATETIME_AMOUNT'
  | 'DATETIME_AMOUNT_UNIQUE'
  | 'FALLBACK_AMOUNT_DAY_BRAND'
  | 'DATE_AMOUNT_METHOD'
  | 'PIX_NSU_AMOUNT'
  | 'PIX_DATETIME_AMOUNT'
  | 'CANON_PIX_TOLERANCE'
  | 'CANON_L1'
  | 'CANON_L2'
  | 'CANON_L3'
  | 'CANON_L4'
  | 'CANON_EXCLUSIVE_L1'
  | 'CANON_EXCLUSIVE_L2'
  | 'CANON_EXCLUSIVE_L0'
  | 'CANON_FLEX_L5';

type CandidateEntry = {
  acquirer: 'CIELO' | 'SIPAG' | 'SICREDI';
  candidate: CieloSale | SipagSale | SicrediSale;
  score: number;
  timeDiffMs: number;
  amountDiffAbs: number;
};
