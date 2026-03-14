import { FormEvent, useMemo, useState } from 'react';
import { useMutation, useQuery } from '@tanstack/react-query';
import axios from 'axios';
import { AuthUser, fetchAuthSession, loginAuth, logoutAuth, setupAuth } from '../../api/auth';
import { queryClient } from '../../app/queryClient';
import { Card } from '../common/Card';
import { Button } from '../common/Button';
import { Input } from '../common/Input';
import { Spinner } from '../common/Spinner';

type AuthGateProps = {
  children: (params: {
    user: AuthUser;
    logout: () => Promise<void>;
    logoutPending: boolean;
  }) => JSX.Element;
};

export const AuthGate = ({ children }: AuthGateProps) => {
  const authQuery = useQuery({
    queryKey: ['auth-session'],
    queryFn: fetchAuthSession,
    retry: false,
  });
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [errorMessage, setErrorMessage] = useState('');

  const getFriendlyAuthError = (error: unknown, fallback: string) => {
    if (axios.isAxiosError(error)) {
      const apiError = String(error.response?.data?.error ?? '').trim();
      if (apiError === 'invalid_credentials') {
        return 'Usuário ou senha inválidos.';
      }
      if (apiError === 'user_pending_primary_admin_approval') {
        return 'Seu usuário foi criado, mas ainda aguarda aprovação do admin primário.';
      }
      if (apiError === 'auth_already_initialized') {
        return 'O usuário inicial já foi criado. Faça login com uma conta existente.';
      }
      if (apiError === 'username_already_exists') {
        return 'Esse nome de usuário já existe.';
      }
      if (apiError === 'username_invalido') {
        return 'Informe um usuário com pelo menos 3 caracteres.';
      }
      if (apiError === 'password_invalido') {
        return 'Informe uma senha com pelo menos 6 caracteres.';
      }
      if (apiError) {
        return apiError;
      }
    }
    return (error as Error)?.message || fallback;
  };

  const setupMode = useMemo(() => authQuery.data?.initialized === false, [authQuery.data?.initialized]);

  const loginMutation = useMutation({
    mutationFn: loginAuth,
    onSuccess: async () => {
      setErrorMessage('');
      setPassword('');
      await queryClient.invalidateQueries({ queryKey: ['auth-session'] });
    },
    onError: (error) => {
      setErrorMessage(getFriendlyAuthError(error, 'Falha ao iniciar sessão.'));
    },
  });

  const setupMutation = useMutation({
    mutationFn: setupAuth,
    onSuccess: async () => {
      setErrorMessage('');
      setPassword('');
      await queryClient.invalidateQueries({ queryKey: ['auth-session'] });
    },
    onError: (error) => {
      setErrorMessage(getFriendlyAuthError(error, 'Falha ao criar o usuário inicial.'));
    },
  });

  const logoutMutation = useMutation({
    mutationFn: logoutAuth,
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['auth-session'] });
      queryClient.removeQueries({ queryKey: ['dashboard-finance'] });
    },
  });

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setErrorMessage('');
    try {
      if (setupMode) {
        await setupMutation.mutateAsync({
          username,
          password,
          displayName: displayName.trim() || undefined,
        });
        return;
      }
      await loginMutation.mutateAsync({ username, password });
    } catch {
      // handled by mutation onError
    }
  };

  if (authQuery.isLoading) {
    return (
      <div className="flex min-h-[40vh] items-center justify-center">
        <Spinner />
      </div>
    );
  }

  const user = authQuery.data?.authenticated ? authQuery.data.user : null;
  if (user) {
    return children({
      user,
      logout: async () => {
        await logoutMutation.mutateAsync();
      },
      logoutPending: logoutMutation.isPending,
    });
  }

  return (
    <div className="flex min-h-[calc(100vh-6rem)] items-center justify-center py-10">
      <Card className="w-full max-w-md border border-slate-200 bg-white/95 p-6 shadow-xl">
        <div className="mb-6">
          <p className="text-xs uppercase tracking-[0.24em] text-slate-500">Supermercado Tigre</p>
          <h1 className="mt-2 text-2xl font-semibold text-slate-900">
            {setupMode ? 'Criar usuário administrador' : 'Iniciar sessão'}
          </h1>
          <p className="mt-2 text-sm text-slate-500">
            {setupMode
              ? 'Nenhum usuário foi encontrado no Firebird. Cadastre o primeiro acesso administrativo.'
              : 'Entre com usuário e senha para acessar o app.'}
          </p>
        </div>
        <form className="space-y-4" onSubmit={handleSubmit}>
          {setupMode ? (
            <div>
              <label className="mb-1 block text-xs font-semibold uppercase tracking-wide text-slate-500">
                Nome de exibição
              </label>
              <Input
                value={displayName}
                onChange={(event) => setDisplayName(event.target.value)}
                placeholder="Administrador"
                autoComplete="name"
              />
            </div>
          ) : null}
          <div>
            <label className="mb-1 block text-xs font-semibold uppercase tracking-wide text-slate-500">
              Usuário
            </label>
            <Input
              value={username}
              onChange={(event) => setUsername(event.target.value)}
              placeholder="ADMIN"
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
              placeholder="Sua senha"
              autoComplete={setupMode ? 'new-password' : 'current-password'}
            />
          </div>
          {errorMessage ? <p className="text-sm text-rose-600">{errorMessage}</p> : null}
          <Button
            type="submit"
            className="w-full"
            disabled={loginMutation.isPending || setupMutation.isPending}
          >
            {setupMutation.isPending || loginMutation.isPending
              ? 'Processando...'
              : setupMode
                ? 'Criar e entrar'
                : 'Entrar'}
          </Button>
        </form>
      </Card>
    </div>
  );
};
