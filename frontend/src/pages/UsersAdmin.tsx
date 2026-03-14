import { useMemo, useState } from 'react';
import { useMutation, useQuery } from '@tanstack/react-query';
import { useOutletContext } from 'react-router-dom';
import { approveAdminUser, createAdminUser, listAdminUsers } from '../api/auth';
import { queryClient } from '../app/queryClient';
import { ShellOutletContext } from '../app/outletContext';
import { Button } from '../components/common/Button';
import { Card } from '../components/common/Card';
import { EmptyState } from '../components/common/EmptyState';
import { Input } from '../components/common/Input';
import { Spinner } from '../components/common/Spinner';

const formatDateTime = (value?: string | null) => {
  if (!value) {
    return '--';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  const pad = (num: number) => String(num).padStart(2, '0');
  return `${pad(date.getDate())}/${pad(date.getMonth() + 1)}/${date.getFullYear()} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
};

export const UsersAdmin = () => {
  const { currentUser, logout, logoutPending } = useOutletContext<ShellOutletContext>();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [formError, setFormError] = useState('');

  const usersQuery = useQuery({
    queryKey: ['admin-users'],
    queryFn: listAdminUsers,
  });

  const createMutation = useMutation({
    mutationFn: createAdminUser,
    onSuccess: async () => {
      setUsername('');
      setPassword('');
      setDisplayName('');
      setFormError('');
      await queryClient.invalidateQueries({ queryKey: ['admin-users'] });
    },
    onError: (error) => {
      setFormError((error as Error)?.message || 'Falha ao criar usuário.');
    },
  });

  const approveMutation = useMutation({
    mutationFn: approveAdminUser,
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['admin-users'] });
      await queryClient.invalidateQueries({ queryKey: ['auth-session'] });
    },
  });

  const users = usersQuery.data?.users ?? [];
  const pendingUsers = useMemo(() => users.filter((user) => !user.isApproved), [users]);
  const approvedUsers = useMemo(() => users.filter((user) => user.isApproved), [users]);

  return (
    <div className="flex flex-col gap-4">
      <header>
        <h1 className="text-2xl font-semibold text-slate-900">Usuários</h1>
        <p className="text-sm text-slate-500">
          Crie novos acessos e aprove pendências pelo admin primário.
        </p>
      </header>

      <Card className="p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs uppercase tracking-[0.24em] text-slate-500">Sessão atual</p>
            <p className="mt-1 text-sm text-slate-700">
              {currentUser.displayName || currentUser.username}
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <span className="rounded-full border border-slate-300 px-3 py-1 text-xs font-semibold uppercase tracking-wide text-slate-600">
              {currentUser.isPrimaryAdmin ? 'Admin primário' : 'Usuário autenticado'}
            </span>
            <span className="rounded-full border border-emerald-300 bg-emerald-50 px-3 py-1 text-xs font-semibold uppercase tracking-wide text-emerald-700">
              {currentUser.isApproved ? 'Aprovado' : 'Pendente'}
            </span>
            <Button
              variant="outline"
              disabled={logoutPending}
              onClick={() => {
                void logout();
              }}
            >
              {logoutPending ? 'Saindo...' : 'Sair'}
            </Button>
          </div>
        </div>
      </Card>

      <Card className="p-4">
        <div className="mb-4">
          <p className="text-xs uppercase tracking-[0.24em] text-slate-500">Criar usuário</p>
          <p className="mt-1 text-sm text-slate-500">
            O novo usuário entra como pendente até o admin primário aprovar.
          </p>
        </div>
        <form
          className="grid grid-cols-1 gap-3 md:grid-cols-4"
          onSubmit={(event) => {
            event.preventDefault();
            setFormError('');
            void createMutation.mutateAsync({
              username,
              password,
              displayName: displayName.trim() || undefined,
            });
          }}
        >
          <div>
            <label className="mb-1 block text-xs font-semibold uppercase tracking-wide text-slate-500">
              Nome
            </label>
            <Input
              value={displayName}
              onChange={(event) => setDisplayName(event.target.value)}
              placeholder="Ex.: Maria Souza"
              autoComplete="name"
            />
          </div>
          <div>
            <label className="mb-1 block text-xs font-semibold uppercase tracking-wide text-slate-500">
              Usuário
            </label>
            <Input
              value={username}
              onChange={(event) => setUsername(event.target.value)}
              placeholder="OPERADOR01"
              autoComplete="username"
            />
          </div>
          <div>
            <label className="mb-1 block text-xs font-semibold uppercase tracking-wide text-slate-500">
              Senha
            </label>
            <Input
              type="password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              placeholder="Mínimo 6 caracteres"
              autoComplete="new-password"
            />
          </div>
          <div className="flex items-end">
            <Button type="submit" className="w-full" disabled={createMutation.isPending}>
              {createMutation.isPending ? 'Criando...' : 'Criar usuário'}
            </Button>
          </div>
        </form>
        {formError ? <p className="mt-3 text-sm text-rose-600">{formError}</p> : null}
      </Card>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
        <Card className="p-4">
          <div className="mb-4 flex items-center justify-between gap-2">
            <div>
              <p className="text-xs uppercase tracking-[0.24em] text-slate-500">Pendentes</p>
              <p className="mt-1 text-sm text-slate-500">
                Somente o primeiro usuário criado pode aprovar.
              </p>
            </div>
            <span className="rounded-full border border-amber-300 bg-amber-50 px-3 py-1 text-xs font-semibold uppercase tracking-wide text-amber-700">
              {pendingUsers.length} pendente(s)
            </span>
          </div>
          {usersQuery.isLoading ? (
            <Spinner />
          ) : usersQuery.isError ? (
            <p className="text-sm text-rose-600">
              {(usersQuery.error as Error)?.message || 'Falha ao carregar usuários.'}
            </p>
          ) : pendingUsers.length ? (
            <div className="space-y-3">
              {pendingUsers.map((user) => (
                <div
                  key={user.id}
                  className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3"
                >
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <p className="text-sm font-semibold text-slate-800">
                        {user.displayName || user.username}
                      </p>
                      <p className="text-xs uppercase tracking-wide text-slate-500">
                        {user.username}
                      </p>
                      <p className="mt-2 text-xs text-slate-500">
                        Criado em {formatDateTime(user.createdAt)}
                      </p>
                    </div>
                    <Button
                      variant="outline"
                      disabled={!currentUser.isPrimaryAdmin || approveMutation.isPending}
                      onClick={() => {
                        void approveMutation.mutateAsync(user.id);
                      }}
                    >
                      {approveMutation.isPending ? 'Aprovando...' : 'Aprovar'}
                    </Button>
                  </div>
                  {!currentUser.isPrimaryAdmin ? (
                    <p className="mt-3 text-xs text-amber-700">
                      Aprovação restrita ao admin primário.
                    </p>
                  ) : null}
                </div>
              ))}
            </div>
          ) : (
            <EmptyState
              title="Sem usuários pendentes"
              description="Novos acessos criados aparecerão aqui até serem aprovados."
            />
          )}
        </Card>

        <Card className="p-4">
          <div className="mb-4 flex items-center justify-between gap-2">
            <div>
              <p className="text-xs uppercase tracking-[0.24em] text-slate-500">Aprovados</p>
              <p className="mt-1 text-sm text-slate-500">
                Usuários já autorizados a entrar no app.
              </p>
            </div>
            <span className="rounded-full border border-emerald-300 bg-emerald-50 px-3 py-1 text-xs font-semibold uppercase tracking-wide text-emerald-700">
              {approvedUsers.length} aprovado(s)
            </span>
          </div>
          {usersQuery.isLoading ? (
            <Spinner />
          ) : approvedUsers.length ? (
            <div className="space-y-3">
              {approvedUsers.map((user) => (
                <div
                  key={user.id}
                  className="rounded-2xl border border-slate-200 bg-white px-4 py-3"
                >
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <p className="text-sm font-semibold text-slate-800">
                        {user.displayName || user.username}
                      </p>
                      <p className="text-xs uppercase tracking-wide text-slate-500">
                        {user.username}
                      </p>
                    </div>
                    <div className="flex flex-wrap gap-2">
                      {user.isPrimaryAdmin ? (
                        <span className="rounded-full border border-slate-300 px-2 py-1 text-[11px] font-semibold uppercase tracking-wide text-slate-700">
                          Admin primário
                        </span>
                      ) : null}
                      <span className="rounded-full border border-emerald-300 bg-emerald-50 px-2 py-1 text-[11px] font-semibold uppercase tracking-wide text-emerald-700">
                        Aprovado
                      </span>
                    </div>
                  </div>
                  <div className="mt-3 grid grid-cols-1 gap-2 text-xs text-slate-500 md:grid-cols-2">
                    <span>Criado em {formatDateTime(user.createdAt)}</span>
                    <span>Aprovado em {formatDateTime(user.approvedAt)}</span>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState
              title="Nenhum usuário aprovado"
              description="Depois da aprovação, os usuários aparecerão nesta lista."
            />
          )}
        </Card>
      </div>
    </div>
  );
};
