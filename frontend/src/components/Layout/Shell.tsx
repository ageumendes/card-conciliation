import { useEffect, useState } from 'react';
import { Outlet } from 'react-router-dom';
import { Topbar } from './Topbar';
import { AuthGate } from '../Auth/AuthGate';
import { ReconciliationBlockingOverlay } from '../Reconciliation/ReconciliationBlockingOverlay';

type ThemeMode = 'light' | 'dark';
const THEME_STORAGE_KEY = 'ui-theme-mode';

const resolveInitialTheme = (): ThemeMode => {
  if (typeof window === 'undefined') {
    return 'light';
  }
  const stored = window.localStorage.getItem(THEME_STORAGE_KEY);
  if (stored === 'light' || stored === 'dark') {
    return stored;
  }
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
};

export const Shell = () => {
  const [theme, setTheme] = useState<ThemeMode>(() => resolveInitialTheme());

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    window.localStorage.setItem(THEME_STORAGE_KEY, theme);
  }, [theme]);

  return (
    <AuthGate>
      {({ user, logout, logoutPending }) => (
        <div className="fixed inset-0 flex min-h-0 min-w-0 flex-col overflow-hidden bg-canvas text-ink">
          <ReconciliationBlockingOverlay />
          <Topbar
            theme={theme}
            onToggleTheme={() => setTheme((prev) => (prev === 'light' ? 'dark' : 'light'))}
          />
          <main className="mx-auto flex w-full max-w-[1400px] flex-1 min-h-0 min-w-0 flex-col overflow-x-hidden overflow-y-auto px-3 py-1 sm:px-6">
            <Outlet
              context={{
                currentUser: user,
                logout,
                logoutPending,
              }}
            />
          </main>
        </div>
      )}
    </AuthGate>
  );
};
