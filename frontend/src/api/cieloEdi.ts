import { http } from './http';
import { endpoints } from './endpoints';
import { ApiFilesResponse, ApiPingResponse } from './types';

export const fetchCieloEdiPing = async () => {
  const { data } = await http.get<ApiPingResponse>(endpoints.cieloEdi.ping);
  return data;
};

export const fetchCieloEdiFiles = async () => {
  const { data } = await http.get<ApiFilesResponse>(endpoints.cieloEdi.files);
  return data;
};

export const scanCieloEdi = async () => {
  const { data } = await http.post(endpoints.cieloEdi.scan);
  return data;
};

export const syncCieloEdi = async () => {
  const { data } = await http.post(endpoints.cieloEdi.sync);
  return data;
};
