import { http } from './http';
import { endpoints } from './endpoints';
import { ApiFilesResponse, ApiPingResponse, InterdataSale, InterdataSalesResponse } from './types';
import { toNumberSafe, toText } from '../lib/normalize';

export const fetchInterdataPing = async () => {
  const { data } = await http.get<ApiPingResponse>(endpoints.interdata.ping);
  return data;
};

export const fetchInterdataFiles = async () => {
  const { data } = await http.get<ApiFilesResponse>(endpoints.interdata.files);
  return data;
};

export const scanInterdata = async () => {
  const { data } = await http.post(endpoints.interdata.scan);
  return data;
};

export const uploadInterdata = async (
  file: File,
  options?: { onProgress?: (value: number) => void; uploadId?: string },
) => {
  const form = new FormData();
  form.append('file', file);
  const { data } = await http.post(endpoints.interdata.upload, form, {
    onUploadProgress: (event) => {
      if (!event.total) {
        return;
      }
      const percent = Math.round((event.loaded / event.total) * 100);
      options?.onProgress?.(percent);
    },
    headers: options?.uploadId ? { 'X-Upload-Id': options.uploadId } : undefined,
  });
  return data;
};

export const fetchInterdataImportDetails = async (params: {
  metric: 'files' | 'inserted' | 'duplicates' | 'invalid' | 'review' | 'errors';
  limit?: number;
}) => {
  const { data } = await http.get(endpoints.interdata.importDetails, { params });
  return data;
};

export const fetchInterdataSales = async (params: {
  dateFrom?: string;
  dateTo?: string;
  page?: number;
  limit?: number;
  status?: string;
  acquirer?: string;
  search?: string;
  paymentType?: string;
  brand?: string;
  bucket?: string;
  sortBy?: 'datetime' | 'amount';
  sortDir?: 'asc' | 'desc';
}) => {
  const { data } = await http.get<InterdataSalesResponse>(endpoints.interdata.sales, { params });
  const normalized = (data.data ?? []).map((sale) => normalizeSale(sale));
  return { ...data, data: normalized };
};

export const approveInterdataSale = async (payload: { id: number; bucket: 'invalid' | 'duplicate' }) => {
  const { data } = await http.post(endpoints.interdata.approve, payload);
  return data;
};

export const triggerInterdataReconciliation = async (params?: {
  dateFrom?: string;
  dateTo?: string;
}) => {
  const { data } = await http.post(endpoints.interdata.reconcileRun, null, { params });
  return data;
};

const normalizeSale = (sale: InterdataSale): InterdataSale => {
  const normalized: InterdataSale = { ...sale };
  const fieldsToNormalize: Array<keyof InterdataSale> = [
    'STATUS_RAW',
    'STATUS',
    'SALE_NO',
    'AUTH_NSU',
    'CARD_BRAND_RAW',
    'PAYMENT_TYPE',
    'CARD_MODE',
    'ROW_HASH',
    'SOURCE',
  ];

  fieldsToNormalize.forEach((field) => {
    const value = normalized[field];
    if (value !== undefined) {
      normalized[field] = toText(value) as any;
    }
  });

  if (normalized.SALE_DATETIME) {
    normalized.SALE_DATETIME = toText(normalized.SALE_DATETIME);
  }
  if (normalized.CREATED_AT) {
    normalized.CREATED_AT = toText(normalized.CREATED_AT);
  }

  if (normalized.GROSS_AMOUNT !== undefined) {
    const parsed = toNumberSafe(normalized.GROSS_AMOUNT);
    normalized.GROSS_AMOUNT = parsed ?? undefined;
  }
  if (normalized.FEES_AMOUNT !== undefined) {
    const parsed = toNumberSafe(normalized.FEES_AMOUNT);
    normalized.FEES_AMOUNT = parsed ?? undefined;
  }
  if (normalized.NET_AMOUNT !== undefined) {
    const parsed = toNumberSafe(normalized.NET_AMOUNT);
    normalized.NET_AMOUNT = parsed ?? undefined;
  }
  if (normalized.INSTALLMENTS !== undefined) {
    const parsed = toNumberSafe(normalized.INSTALLMENTS);
    normalized.INSTALLMENTS = parsed !== null ? Math.trunc(parsed) : undefined;
  }

  return normalized;
};
