import { http } from './http';
import { endpoints } from './endpoints';

export const clearSipagDev = async () => {
  const { data } = await http.post(endpoints.maintenance.clearSipag, null, {
    params: { confirm: 'YES' },
  });
  return data;
};

export const clearAllDev = async () => {
  const { data } = await http.post(endpoints.maintenance.clearAll, null, {
    params: { confirm: 'YES' },
  });
  return data;
};
