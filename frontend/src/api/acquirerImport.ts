import { http } from './http';
import { endpoints } from './endpoints';
import {
  AcquirerSalesResponse,
  AcquirerUnifiedSale,
  ApiPingResponse,
  CieloSale,
  SicrediSale,
  SipagSale,
} from './types';

export const fetchAcquirerImportPing = async () => {
  const { data } = await http.get<ApiPingResponse>(endpoints.acquirerImport.ping);
  return data;
};

export const uploadAcquirerImport = async (payload: { file: File; acquirer: 'cielo' | 'sipag' | 'sicredi' }) => {
  const form = new FormData();
  form.append('file', payload.file);
  const { data } = await http.post(endpoints.acquirerImport.upload, form, {
    params: { acquirer: payload.acquirer },
  });
  return data;
};

export const fetchAcquirerSales = async (params: {
  acquirer: 'cielo' | 'sipag' | 'sicredi';
  dateFrom?: string;
  dateTo?: string;
  page?: number;
  limit?: number;
  status?: string;
  brand?: string;
  search?: string;
  paymentType?: string;
  sortBy?: 'datetime' | 'amount';
  sortDir?: 'asc' | 'desc';
}) => {
  const { data } = await http.get<AcquirerSalesResponse<CieloSale | SipagSale | SicrediSale>>(
    endpoints.acquirerImport.sales,
    { params },
  );
  return data;
};

export const fetchAcquirerSalesUnified = async (params: {
  acquirers: string;
  dateFrom?: string;
  dateTo?: string;
  page?: number;
  limit?: number;
  search?: string;
  sortBy?: 'datetime' | 'amount';
  sortDir?: 'asc' | 'desc';
  includeReconciled?: boolean;
}) => {
  const { data } = await http.get<AcquirerSalesResponse<AcquirerUnifiedSale>>(
    endpoints.acquirerImport.sales,
    { params },
  );
  return data;
};

export const fetchAcquirerFinance = async (params: {
  acquirers: string;
  dateFrom: string;
  dateTo: string;
  search?: string;
  sortBy?: 'datetime' | 'amount';
  sortDir?: 'asc' | 'desc';
  includeReconciled?: boolean;
}) => {
  const { data } = await http.get<AcquirerSalesResponse<AcquirerUnifiedSale>>(
    endpoints.acquirerImport.finance,
    { params },
  );
  return data;
};
