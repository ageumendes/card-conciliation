import { http } from './http';

export type AuthUser = {
  id: number;
  username: string;
  displayName: string | null;
  isApproved: boolean;
  isPrimaryAdmin: boolean;
};

export type AdminUserRecord = AuthUser & {
  isActive: boolean;
  createdBy: number | null;
  approvedBy: number | null;
  approvedAt: string | null;
  createdAt: string | null;
};

export type AuthSessionResponse = {
  ok: boolean;
  initialized: boolean;
  authenticated: boolean;
  user: AuthUser | null;
};

export const fetchAuthSession = async () => {
  const { data } = await http.get<AuthSessionResponse>('/auth/me');
  return data;
};

export const loginAuth = async (payload: { username: string; password: string }) => {
  const { data } = await http.post<AuthSessionResponse>('/auth/login', payload);
  return data;
};

export const setupAuth = async (payload: {
  username: string;
  password: string;
  displayName?: string;
}) => {
  const { data } = await http.post<AuthSessionResponse>('/auth/setup', payload);
  return data;
};

export const logoutAuth = async () => {
  const { data } = await http.post<{ ok: boolean }>('/auth/logout');
  return data;
};

export const listAdminUsers = async () => {
  const { data } = await http.get<{ ok: boolean; users: AdminUserRecord[] }>('/admin/auth/users');
  return data;
};

export const createAdminUser = async (payload: {
  username: string;
  password: string;
  displayName?: string;
}) => {
  const { data } = await http.post<{ ok: boolean; user: AdminUserRecord }>('/admin/auth/users', payload);
  return data;
};

export const approveAdminUser = async (userId: number) => {
  const { data } = await http.post<{ ok: boolean; user: AdminUserRecord }>(
    `/admin/auth/users/${userId}/approve`,
  );
  return data;
};
