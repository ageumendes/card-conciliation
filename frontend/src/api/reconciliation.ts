import { http } from './http';
import { endpoints } from './endpoints';
import { AuditDuplicatesResponse } from './types';

export const runReconciliation = async (params: {
  dateFrom: string;
  dateTo: string;
  acquirer: 'CIELO' | 'SIPAG';
  limit?: number;
  dryRun?: boolean;
}) => {
  const { data } = await http.post(endpoints.reconciliation.run, null, { params });
  return data;
};

export const fetchReconciliations = async (params: {
  dateFrom?: string;
  dateTo?: string;
  acquirer?: string;
  status?: string;
  paymentType?: string;
  brand?: string;
  search?: string;
  page?: number;
  limit?: number;
  sortBy?: 'datetime' | 'amount';
  sortDir?: 'asc' | 'desc';
}) => {
  const { data } = await http.get(endpoints.reconciliation.list, { params });
  return data;
};

export const reconcileManual = async (payload: {
  interdataId: number;
  acquirerProvider: 'CIELO' | 'SIPAG' | 'CIELO_EDI';
  acquirerSaleId: number;
  reason: 'TIME_DIFF' | 'NSU_DIFF' | 'BATCH' | 'OTHER';
  notes?: string;
}) => {
  const { data } = await http.post(endpoints.reconciliation.manual, payload);
  return data;
};

export const fetchReconciliationDetails = async (id: number) => {
  const { data } = await http.get(endpoints.reconciliation.details(id));
  return data;
};

export const fetchReconciliationStatus = async () => {
  const { data } = await http.get<{
    ok: boolean;
    data: {
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
  }>(endpoints.reconciliation.status);
  return data;
};

export const getAuditDuplicates = async (params: {
  acquirer: 'all' | 'cielo' | 'sipag' | 'sicredi';
  from?: string;
  to?: string;
  onlySuspicious?: boolean;
}) => {
  const { data } = await http.get<AuditDuplicatesResponse>(
    endpoints.reconciliation.auditDuplicates,
    { params },
  );
  return data;
};
