import { Injectable } from '@nestjs/common';

export type ReconciliationRuntimeStatus = {
  running: boolean;
  source: string;
  startedAt: string | null;
  finishedAt: string | null;
  processed: number;
  total: number;
  matched: number;
  pending: number;
  errors: number;
  dryRun: boolean;
  verbose: boolean;
  message: string;
  updatedAt: string | null;
  lastSummary: Record<string, unknown> | null;
};

@Injectable()
export class ReconciliationStatusService {
  private status: ReconciliationRuntimeStatus = {
    running: false,
    source: 'idle',
    startedAt: null,
    finishedAt: null,
    processed: 0,
    total: 0,
    matched: 0,
    pending: 0,
    errors: 0,
    dryRun: false,
    verbose: false,
    message: 'Conciliacao ociosa.',
    updatedAt: null,
    lastSummary: null,
  };

  start(payload: {
    source: string;
    total: number;
    dryRun: boolean;
    verbose: boolean;
    message?: string;
  }) {
    const now = new Date().toISOString();
    this.status = {
      running: true,
      source: payload.source,
      startedAt: now,
      finishedAt: null,
      processed: 0,
      total: payload.total,
      matched: 0,
      pending: 0,
      errors: 0,
      dryRun: payload.dryRun,
      verbose: payload.verbose,
      message: payload.message ?? 'Conciliacao em andamento...',
      updatedAt: now,
      lastSummary: this.status.lastSummary,
    };
  }

  update(payload: Partial<Pick<ReconciliationRuntimeStatus, 'processed' | 'matched' | 'pending' | 'errors' | 'message'>>) {
    const now = new Date().toISOString();
    this.status = {
      ...this.status,
      ...payload,
      updatedAt: now,
    };
  }

  finish(summary: Record<string, unknown>, message = 'Conciliacao finalizada.') {
    const now = new Date().toISOString();
    this.status = {
      ...this.status,
      running: false,
      finishedAt: now,
      updatedAt: now,
      message,
      lastSummary: summary,
    };
  }

  fail(summary: Record<string, unknown>, message = 'Conciliacao finalizada com erro.') {
    this.finish(summary, message);
  }

  getStatus() {
    return this.status;
  }
}
