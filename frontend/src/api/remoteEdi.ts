import { http } from './http';
import { endpoints } from './endpoints';

export type RemoteEdiPullPayload = {
  cielo?: boolean;
  sipag?: boolean;
  sicredi?: boolean;
  dryRun?: boolean;
  moveUnknownToError?: boolean;
};

export const pullRemoteEdis = async (payload: RemoteEdiPullPayload) => {
  const { data } = await http.post(endpoints.remoteEdi.pull, payload);
  return data;
};

export const fetchRemoteEdiPing = async () => {
  const { data } = await http.get(endpoints.remoteEdi.ping);
  return data;
};
