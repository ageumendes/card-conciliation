import { AuthUser } from '../api/auth';

export type ShellOutletContext = {
  currentUser: AuthUser;
  logout: () => Promise<void>;
  logoutPending: boolean;
};
