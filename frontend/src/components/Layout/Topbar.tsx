import { NavLink } from 'react-router-dom';
import clsx from 'clsx';

const links = [
  { to: '/', label: 'Financeiro' },
  { to: '/reconciliation', label: 'Conciliacao' },
  { to: '/imports', label: 'Importar Arquivos' },
  { to: '/users', label: 'Usuários' },
];

type TopbarProps = {
  theme: 'light' | 'dark';
  onToggleTheme: () => void;
};

export const Topbar = ({
  theme,
  onToggleTheme,
}: TopbarProps) => {
  const isDark = theme === 'dark';

  return (
    <header className="sticky top-0 z-30 w-full min-w-0 overflow-x-hidden border-b border-slate-200 bg-white/90 backdrop-blur">
      <div className="mx-auto flex w-full min-w-0 max-w-[1400px] flex-wrap items-center justify-between gap-2 px-3 py-1 sm:px-6">
        <div className="min-w-0">
          <p className="text-lg font-semibold text-slate-800">Consiliação de Cartões</p>
          <p className="text-xs uppercase tracking-[0.24em] text-slate-500">Supermercado Tigre</p>
        </div>
        <nav className="flex min-w-0 max-w-full flex-wrap items-center justify-end gap-2">
          {links.map((link) => (
            <NavLink
              key={link.to}
              to={link.to}
              className={({ isActive }) =>
                clsx(
                  'rounded-full px-3 py-2 text-sm font-medium transition',
                  isActive
                    ? isDark
                      ? 'bg-slate-600 text-slate-100 shadow-sm'
                      : 'bg-slate-700 text-slate-100 shadow-sm'
                    : 'text-slate-700 hover:bg-slate-100',
                )
              }
            >
              {link.label}
            </NavLink>
          ))}
          <button
            type="button"
            onClick={onToggleTheme}
            className="theme-toggle"
            title={isDark ? "Alternar para tema Claro" : "Alternar para tema Escuro"}
            aria-label={isDark ? 'Alternar para tema Claro' : 'Alternar para tema Escuro'}
          >
            <span
              aria-hidden="true"
              className={clsx('theme-toggle__icon', isDark ? 'theme-toggle__icon--moon' : 'theme-toggle__icon--sun')}
            />
          </button>
        </nav>
      </div>
    </header>
  );
};
