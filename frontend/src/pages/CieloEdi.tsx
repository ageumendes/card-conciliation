import { useMutation, useQuery } from '@tanstack/react-query';
import { useState } from 'react';
import { fetchAcquirerImportPing, uploadAcquirerImport } from '../api/acquirerImport';
import { pullRemoteEdis } from '../api/remoteEdi';
import { Card } from '../components/common/Card';
import { Button } from '../components/common/Button';
import { Spinner } from '../components/common/Spinner';
import { EmptyState } from '../components/common/EmptyState';
import { queryClient } from '../app/queryClient';

type RemoteProviderSummary = {
  listed: number;
  skippedRecent: number;
  downloaded: number;
  imported: number;
  movedProcessed: number;
  movedError: number;
  ignored: number;
  errors: string[];
};

type RemotePullData = {
  ok: boolean;
  host: string;
  startedAt: string;
  finishedAt: string;
  options: {
    cielo: boolean;
    sipag: boolean;
    sicredi: boolean;
    dryRun: boolean;
    moveUnknownToError: boolean;
  };
  summary: {
    cielo: RemoteProviderSummary;
    sipag: RemoteProviderSummary;
    sicredi: RemoteProviderSummary;
    total: {
      listed: number;
      downloaded: number;
      movedProcessed: number;
      movedError: number;
      ignored: number;
    };
  };
};

type RemotePullResponse = {
  ok: boolean;
  data: RemotePullData;
};

const acquirerLabels = {
  cielo: 'CIELO',
  sipag: 'SIPAG',
  sicredi: 'SICREDI',
} as const;

const formatDateTime = (value?: string) => {
  if (!value) {
    return '-';
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat('pt-BR', {
    dateStyle: 'short',
    timeStyle: 'medium',
  }).format(parsed);
};

const formatDuration = (startedAt?: string, finishedAt?: string) => {
  if (!startedAt || !finishedAt) {
    return '-';
  }

  const started = new Date(startedAt).getTime();
  const finished = new Date(finishedAt).getTime();
  if (Number.isNaN(started) || Number.isNaN(finished) || finished < started) {
    return '-';
  }

  const totalSeconds = Math.round((finished - started) / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`;
};

export const CieloEdi = () => {
  const pingQuery = useQuery({ queryKey: ['acquirer-import-ping'], queryFn: fetchAcquirerImportPing });
  const [acquirer, setAcquirer] = useState<'cielo' | 'sipag' | 'sicredi'>('cielo');
  const [lastResult, setLastResult] = useState<null | Record<string, unknown>>(null);
  const [remoteResult, setRemoteResult] = useState<RemotePullResponse | null>(null);
  const [remoteError, setRemoteError] = useState<string | null>(null);
  const [showRemoteRaw, setShowRemoteRaw] = useState(false);
  const [moveUnknownToError, setMoveUnknownToError] = useState(false);
  const [remoteTargets, setRemoteTargets] = useState({
    cielo: true,
    sipag: true,
    sicredi: true,
  });

  const uploadMutation = useMutation({
    mutationFn: uploadAcquirerImport,
    onSuccess: (data) => {
      setLastResult(data);
      queryClient.invalidateQueries({ queryKey: ['acquirer-import-ping'] });
    },
  });

  const remotePullMutation = useMutation({
    mutationFn: pullRemoteEdis,
    onMutate: () => {
      setRemoteError(null);
    },
    onSuccess: (data) => {
      setRemoteResult(data);
      queryClient.invalidateQueries({ queryKey: ['acquirer-import-ping'] });
    },
    onError: (error) => {
      setRemoteError((error as Error)?.message || 'Falha ao buscar EDIs no server-SFTP.');
    },
  });

  const toggleRemoteTarget = (key: 'cielo' | 'sipag' | 'sicredi') => {
    setRemoteTargets((prev) => ({
      ...prev,
      [key]: !prev[key],
    }));
  };

  const remotePullData = remoteResult?.data ?? null;
  const selectedAcquirers = Object.entries(remoteTargets)
    .filter(([, enabled]) => enabled)
    .map(([key]) => acquirerLabels[key as keyof typeof acquirerLabels]);

  return (
    <div className="flex flex-col gap-4">
      <header>
        <h1 className="text-2xl font-semibold text-slate-900">Importacao de adquirentes</h1>
        <p className="text-sm text-slate-500">Upload manual de arquivos Cielo, Sipag e Sicredi.</p>
      </header>

      <Card className="p-4">
        <p className="text-xs uppercase text-slate-400">Status</p>
        {pingQuery.isLoading ? (
          <Spinner />
        ) : pingQuery.isError ? (
          <p className="mt-2 text-sm text-rose-600">
            {(pingQuery.error as Error)?.message || 'Falha ao carregar o status.'}
          </p>
        ) : (
          <div className="mt-2 text-sm text-slate-600">
            <p>Adquirentes: {(pingQuery.data as any)?.acquirers?.join(', ') || '-'}</p>
          </div>
        )}
      </Card>

      <Card className="p-4">
        <p className="mb-2 text-xs uppercase text-slate-400">Sincronizacao server-SFTP</p>
        <div className="flex flex-wrap items-center gap-3">
          <label className="inline-flex items-center gap-2 text-sm text-slate-700">
            <input
              type="checkbox"
              checked={remoteTargets.cielo}
              onChange={() => toggleRemoteTarget('cielo')}
            />
            CIELO
          </label>
          <label className="inline-flex items-center gap-2 text-sm text-slate-700">
            <input
              type="checkbox"
              checked={remoteTargets.sipag}
              onChange={() => toggleRemoteTarget('sipag')}
            />
            SIPAG
          </label>
          <label className="inline-flex items-center gap-2 text-sm text-slate-700">
            <input
              type="checkbox"
              checked={remoteTargets.sicredi}
              onChange={() => toggleRemoteTarget('sicredi')}
            />
            SICREDI
          </label>
          <label className="inline-flex items-center gap-2 text-sm text-slate-700">
            <input
              type="checkbox"
              checked={moveUnknownToError}
              onChange={() => setMoveUnknownToError((prev) => !prev)}
            />
            Mover desconhecidos para ERROR
          </label>
          <Button
            onClick={() => {
              setRemoteResult(null);
              remotePullMutation.mutate({
                cielo: remoteTargets.cielo,
                sipag: remoteTargets.sipag,
                sicredi: remoteTargets.sicredi,
                moveUnknownToError,
              });
            }}
            disabled={remotePullMutation.isPending}
          >
            {remotePullMutation.isPending ? 'Buscando...' : 'Buscar EDIs no server-SFTP'}
          </Button>
        </div>
        {remotePullMutation.isPending ? <Spinner /> : null}
        {remoteError ? <p className="mt-2 text-xs text-rose-600">{remoteError}</p> : null}
        {remotePullData ? (
          <div className="mt-4 space-y-4">
            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <p className="text-xs uppercase tracking-[0.18em] text-slate-400">Resultado</p>
                  <h3 className="mt-1 text-lg font-semibold text-slate-900">
                    {remotePullData.ok ? 'Sincronizacao concluida' : 'Sincronizacao com alertas'}
                  </h3>
                  <p className="mt-1 text-sm text-slate-500">
                    Host {remotePullData.host} • alvo {selectedAcquirers.join(', ') || 'nenhum'}
                  </p>
                </div>
                <div
                  className={`rounded-full px-3 py-1 text-xs font-semibold ${
                    remotePullData.ok
                      ? 'bg-emerald-100 text-emerald-700'
                      : 'bg-amber-100 text-amber-700'
                  }`}
                >
                  {remotePullData.ok ? 'OK' : 'ATENCAO'}
                </div>
              </div>

              <div className="mt-4 grid gap-3 md:grid-cols-4">
                <div className="rounded-xl border border-slate-200 bg-white p-3">
                  <p className="text-panel-muted text-xs uppercase">Inicio</p>
                  <p className="text-panel-strong mt-1 text-sm font-semibold">
                    {formatDateTime(remotePullData.startedAt)}
                  </p>
                </div>
                <div className="rounded-xl border border-slate-200 bg-white p-3">
                  <p className="text-panel-muted text-xs uppercase">Fim</p>
                  <p className="text-panel-strong mt-1 text-sm font-semibold">
                    {formatDateTime(remotePullData.finishedAt)}
                  </p>
                </div>
                <div className="rounded-xl border border-slate-200 bg-white p-3">
                  <p className="text-panel-muted text-xs uppercase">Duracao</p>
                  <p className="text-panel-strong mt-1 text-sm font-semibold">
                    {formatDuration(remotePullData.startedAt, remotePullData.finishedAt)}
                  </p>
                </div>
                <div className="rounded-xl border border-slate-200 bg-white p-3">
                  <p className="text-panel-muted text-xs uppercase">Baixados</p>
                  <p className="text-panel-strong mt-1 text-sm font-semibold">
                    {remotePullData.summary.total.downloaded}
                  </p>
                </div>
              </div>
            </div>

            <div className="grid gap-3 lg:grid-cols-3">
              {(['cielo', 'sipag', 'sicredi'] as const).map((providerKey) => {
                const provider = remotePullData.summary[providerKey];
                const hasActivity =
                  provider.listed > 0 ||
                  provider.downloaded > 0 ||
                  provider.movedProcessed > 0 ||
                  provider.ignored > 0;
                const hasErrors = provider.errors.length > 0;

                return (
                  <div
                    key={providerKey}
                    className={`rounded-2xl border p-4 ${
                      hasErrors
                        ? 'border-rose-200 bg-rose-50'
                        : hasActivity
                          ? 'border-emerald-200 bg-emerald-50'
                          : 'border-slate-200 bg-white'
                    }`}
                  >
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <p className="text-xs uppercase tracking-[0.18em] text-slate-400">Adquirente</p>
                        <h4 className="mt-1 text-base font-semibold text-slate-900">
                          {acquirerLabels[providerKey]}
                        </h4>
                      </div>
                      <div
                        className={`rounded-full px-3 py-1 text-xs font-semibold ${
                          hasErrors
                            ? 'bg-rose-100 text-rose-700'
                            : hasActivity
                              ? 'bg-emerald-100 text-emerald-700'
                              : 'bg-slate-100 text-slate-600'
                        }`}
                      >
                        {hasErrors ? 'ERROS' : hasActivity ? 'PROCESSADO' : 'SEM MOVIMENTO'}
                      </div>
                    </div>

                    <div className="mt-4 grid grid-cols-2 gap-2 text-sm">
                      <div className="rounded-xl bg-white/80 p-3">
                        <p className="text-panel-muted text-xs uppercase">Encontrados</p>
                        <p className="text-panel-strong mt-1 text-lg font-bold">{provider.listed}</p>
                      </div>
                      <div className="rounded-xl bg-white/80 p-3">
                        <p className="text-panel-muted text-xs uppercase">Baixados</p>
                        <p className="text-panel-strong mt-1 text-lg font-bold">{provider.downloaded}</p>
                      </div>
                      <div className="rounded-xl bg-white/80 p-3">
                        <p className="text-panel-muted text-xs uppercase">Processed</p>
                        <p className="text-panel-strong mt-1 text-lg font-bold">{provider.movedProcessed}</p>
                      </div>
                      <div className="rounded-xl bg-white/80 p-3">
                        <p className="text-panel-muted text-xs uppercase">Ignorados</p>
                        <p className="text-panel-strong mt-1 text-lg font-bold">{provider.ignored}</p>
                      </div>
                    </div>

                    {provider.skippedRecent > 0 ? (
                      <p className="mt-3 text-xs text-slate-500">
                        Arquivos recentes pulados: {provider.skippedRecent}
                      </p>
                    ) : null}
                    {provider.movedError > 0 ? (
                      <p className="mt-1 text-xs text-slate-500">Movidos para error: {provider.movedError}</p>
                    ) : null}
                    {hasErrors ? (
                      <div className="mt-3 rounded-xl border border-rose-200 bg-white p-3 text-xs text-rose-700">
                        {provider.errors.map((error, index) => (
                          <p key={`${providerKey}-${index}`}>{error}</p>
                        ))}
                      </div>
                    ) : null}
                  </div>
                );
              })}
            </div>

            <div className="flex flex-wrap items-center gap-3">
              <Button variant="outline" onClick={() => setShowRemoteRaw((prev) => !prev)}>
                {showRemoteRaw ? 'Ocultar JSON bruto' : 'Mostrar JSON bruto'}
              </Button>
              <p className="text-xs text-slate-500">
                Total: {remotePullData.summary.total.listed} encontrados,{' '}
                {remotePullData.summary.total.downloaded} baixados,{' '}
                {remotePullData.summary.total.movedProcessed} movidos para processed.
              </p>
            </div>

            {showRemoteRaw ? (
              <pre className="overflow-auto rounded-2xl border border-slate-200 bg-slate-950 p-4 text-xs text-slate-100">
                {JSON.stringify(remoteResult, null, 2)}
              </pre>
            ) : null}
          </div>
        ) : null}
      </Card>

      <Card className="p-4">
        <div className="flex flex-wrap items-center gap-2">
          <label className="text-xs uppercase text-slate-400">Adquirente</label>
          <select
            className="rounded-full border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700"
            value={acquirer}
            onChange={(event) => setAcquirer(event.target.value as 'cielo' | 'sipag' | 'sicredi')}
          >
            <option value="cielo">Cielo</option>
            <option value="sipag">Sipag</option>
            <option value="sicredi">Sicredi</option>
          </select>
          <label className="inline-flex cursor-pointer items-center gap-2 rounded-full border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700">
            Upload CSV
            <input
              type="file"
              accept=".csv"
              className="hidden"
              onChange={(event) => {
                const file = event.target.files?.[0];
                if (file) {
                  uploadMutation.mutate({ file, acquirer });
                }
                event.currentTarget.value = '';
              }}
            />
          </label>
        </div>
        {uploadMutation.isPending && <Spinner />}
        {uploadMutation.isError ? (
          <p className="mt-2 text-xs text-rose-600">
            {(uploadMutation.error as Error)?.message || 'Falha ao importar arquivo.'}
          </p>
        ) : null}
      </Card>

      <Card className="p-4">
        <p className="text-xs uppercase text-slate-400">Resultado</p>
        {lastResult ? (
          <pre className="mt-2 text-xs text-slate-600">{JSON.stringify(lastResult, null, 2)}</pre>
        ) : (
          <EmptyState title="Nenhuma importacao ainda" description="Envie um CSV para ver o resultado." />
        )}
      </Card>
    </div>
  );
};
