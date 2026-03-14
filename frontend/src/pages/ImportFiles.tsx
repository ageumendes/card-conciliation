import { useMutation, useQuery } from '@tanstack/react-query';
import { useRef, useState } from 'react';
import { queryClient } from '../app/queryClient';
import { uploadAcquirerImport, fetchAcquirerImportPing } from '../api/acquirerImport';
import {
  fetchInterdataFiles,
  fetchInterdataImportDetails,
  fetchInterdataPing,
  scanInterdata,
  uploadInterdata,
} from '../api/interdata';
import { pullRemoteEdis } from '../api/remoteEdi';
import { Button } from '../components/common/Button';
import { Card } from '../components/common/Card';
import { EmptyState } from '../components/common/EmptyState';
import { Spinner } from '../components/common/Spinner';

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

type ErpImportResult = {
  ok: boolean;
  processedFiles: number;
  insertedSales: number;
  skippedDuplicates: number;
  invalidRows: number;
  invalidSaved: number;
  errors: number;
  uploadId?: string;
  alreadyImported?: boolean;
  fileHash?: string;
  message?: string;
};

type AcquirerImportResult = {
  ok: boolean;
  acquirer: string;
  format: string;
  inserted: number;
  duplicates: number;
  invalidRows: number;
  alreadyImported?: boolean;
  fileHash?: string;
  message?: string;
};

type ActiveOperation =
  | {
      kind: 'erp-upload';
      title: string;
      detail: string;
    }
  | {
      kind: 'acquirer-upload';
      title: string;
      detail: string;
    }
  | {
      kind: 'remote-sync';
      title: string;
      detail: string;
    };

type ErpDetailMetric = 'files' | 'inserted' | 'duplicates' | 'invalid' | 'review' | 'errors';

type ErpDetailResponse = {
  ok: boolean;
  data: {
    metric: ErpDetailMetric;
    items: Array<Record<string, unknown>>;
  };
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

export const ImportFiles = () => {
  const interdataPingQuery = useQuery({ queryKey: ['interdata-ping'], queryFn: fetchInterdataPing });
  const interdataFilesQuery = useQuery({ queryKey: ['interdata-files'], queryFn: fetchInterdataFiles });
  const acquirerPingQuery = useQuery({ queryKey: ['acquirer-import-ping'], queryFn: fetchAcquirerImportPing });

  const [uploadProgress, setUploadProgress] = useState<number | null>(null);
  const [processingProgress, setProcessingProgress] = useState<number | null>(null);
  const [processingMessage, setProcessingMessage] = useState<string | null>(null);
  const [acquirer, setAcquirer] = useState<'cielo' | 'sipag' | 'sicredi'>('cielo');
  const [lastInterdataResult, setLastInterdataResult] = useState<ErpImportResult | null>(null);
  const [lastAcquirerResult, setLastAcquirerResult] = useState<AcquirerImportResult | null>(null);
  const [remoteResult, setRemoteResult] = useState<RemotePullResponse | null>(null);
  const [remoteError, setRemoteError] = useState<string | null>(null);
  const [showRemoteRaw, setShowRemoteRaw] = useState(false);
  const [activeOperation, setActiveOperation] = useState<ActiveOperation | null>(null);
  const [selectedErpMetric, setSelectedErpMetric] = useState<ErpDetailMetric | null>(null);
  const [erpDetailData, setErpDetailData] = useState<ErpDetailResponse['data'] | null>(null);
  const [erpDetailError, setErpDetailError] = useState<string | null>(null);
  const remoteTargets = {
    cielo: true,
    sipag: true,
    sicredi: true,
  };

  const progressRef = useRef<EventSource | null>(null);
  const baseUrl = import.meta.env.VITE_API_BASE_URL || '';

  const closeProgress = () => {
    if (progressRef.current) {
      progressRef.current.close();
      progressRef.current = null;
    }
  };

  const openProgress = (uploadId: string) => {
    closeProgress();
    const params = new URLSearchParams({ uploadId });
    const source = new EventSource(`${baseUrl}/admin/interdata/import/progress?${params.toString()}`, {
      withCredentials: true,
    });
    source.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data) as {
          percent?: number;
          stage?: string;
          message?: string;
        };
        setProcessingProgress(payload.percent ?? 0);
        setProcessingMessage(payload.message ?? null);
        if (payload.stage === 'complete' || payload.stage === 'error') {
          setTimeout(() => closeProgress(), 1000);
        }
      } catch {
        // ignore malformed events
      }
    };
    source.onerror = () => {
      closeProgress();
    };
    progressRef.current = source;
  };

  const createUploadId = () => {
    if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) {
      return crypto.randomUUID();
    }
    return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  };

  const interdataScanMutation = useMutation({
    mutationFn: scanInterdata,
    onSuccess: (data) => {
      setLastInterdataResult(data);
      queryClient.invalidateQueries({ queryKey: ['interdata-files'] });
    },
  });

  const interdataUploadMutation = useMutation({
    mutationFn: (payload: { file: File; uploadId: string }) =>
      uploadInterdata(payload.file, { onProgress: setUploadProgress, uploadId: payload.uploadId }),
    onMutate: (payload) => {
      setActiveOperation({
        kind: 'erp-upload',
        title: 'Importando arquivo ERP',
        detail: payload.file.name,
      });
      setUploadProgress(0);
      setProcessingProgress(0);
      setProcessingMessage(null);
      openProgress(payload.uploadId);
    },
    onSuccess: (data) => {
      setLastInterdataResult(data);
      queryClient.invalidateQueries({ queryKey: ['interdata-files'] });
    },
    onSettled: () => {
      setUploadProgress(null);
      setProcessingProgress(null);
      setProcessingMessage(null);
      closeProgress();
      setActiveOperation(null);
    },
  });

  const acquirerUploadMutation = useMutation({
    mutationFn: uploadAcquirerImport,
    onMutate: (payload) => {
      setActiveOperation({
        kind: 'acquirer-upload',
        title: `Importando arquivo ${payload.acquirer.toUpperCase()}`,
        detail: payload.file.name,
      });
    },
    onSuccess: (data) => {
      setLastAcquirerResult(data);
      queryClient.invalidateQueries({ queryKey: ['acquirer-import-ping'] });
    },
    onSettled: () => {
      setActiveOperation(null);
    },
  });

  const remotePullMutation = useMutation({
    mutationFn: pullRemoteEdis,
    onMutate: () => {
      setActiveOperation({
        kind: 'remote-sync',
        title: 'Sincronizando adquirentes',
        detail: 'Buscando arquivos no server-SFTP',
      });
      setRemoteError(null);
    },
    onSuccess: (data) => {
      setRemoteResult(data);
      queryClient.invalidateQueries({ queryKey: ['acquirer-import-ping'] });
    },
    onError: (error) => {
      setRemoteError((error as Error)?.message || 'Falha ao buscar EDIs no server-SFTP.');
    },
    onSettled: () => {
      setActiveOperation(null);
    },
  });

  const erpDetailsMutation = useMutation({
    mutationFn: (metric: ErpDetailMetric) => fetchInterdataImportDetails({ metric, limit: 20 }),
    onMutate: (metric) => {
      setSelectedErpMetric(metric);
      setErpDetailError(null);
    },
    onSuccess: (data) => {
      setErpDetailData((data as ErpDetailResponse).data);
    },
    onError: (error) => {
      setErpDetailData(null);
      setErpDetailError((error as Error)?.message || 'Falha ao carregar detalhes do ERP.');
    },
  });

  const remotePullData = remoteResult?.data ?? null;
  const selectedAcquirers = Object.entries(remoteTargets)
    .filter(([, enabled]) => enabled)
    .map(([key]) => acquirerLabels[key as keyof typeof acquirerLabels]);
  const erpEnabled = Boolean(interdataPingQuery.data?.enabled);
  const acquirerNames = (acquirerPingQuery.data as any)?.acquirers?.join(', ') || 'CIELO, SIPAG, SICREDI';
  const hasRecentResults = Boolean(lastInterdataResult || lastAcquirerResult || remotePullData);
  const blockingPercent =
    activeOperation?.kind === 'erp-upload'
      ? processingProgress ?? uploadProgress ?? 0
      : null;
  const blockingMessage =
    activeOperation?.kind === 'erp-upload'
      ? processingMessage ??
        (processingProgress !== null
          ? 'Processando e importando registros...'
          : uploadProgress !== null
            ? 'Enviando arquivo para o servidor...'
            : 'Preparando importacao...')
      : activeOperation?.kind === 'acquirer-upload'
        ? 'Validando e importando registros da adquirente...'
        : activeOperation?.kind === 'remote-sync'
          ? 'Baixando e organizando arquivos remotos...'
          : null;
  const erpMetricMeta: Array<{ key: ErpDetailMetric; label: string; value: number }> = lastInterdataResult
    ? [
        { key: 'files', label: 'Arquivos', value: lastInterdataResult.processedFiles },
        { key: 'inserted', label: 'Inseridos', value: lastInterdataResult.insertedSales },
        { key: 'duplicates', label: 'Duplicados', value: lastInterdataResult.skippedDuplicates },
        { key: 'invalid', label: 'Invalidos', value: lastInterdataResult.invalidRows },
        { key: 'review', label: 'Salvos revisao', value: lastInterdataResult.invalidSaved },
        { key: 'errors', label: 'Erros', value: lastInterdataResult.errors },
      ]
    : [];

  const formatValue = (value: unknown) => {
    if (value === null || value === undefined || value === '') {
      return '-';
    }
    if (typeof value === 'number') {
      return new Intl.NumberFormat('pt-BR').format(value);
    }
    const text = String(value);
    const parsedDate = new Date(text);
    if (!Number.isNaN(parsedDate.getTime()) && /T|\d{4}-\d{2}-\d{2}/.test(text)) {
      return formatDateTime(text);
    }
    return text;
  };

  const renderErpDetailContent = () => {
    if (!selectedErpMetric) {
      return null;
    }
    if (erpDetailsMutation.isPending) {
      return (
        <div className="mt-4 flex items-center gap-3 rounded-xl border border-slate-200 bg-slate-50 p-4 text-sm text-slate-500">
          <Spinner />
          Carregando detalhes...
        </div>
      );
    }
    if (erpDetailError) {
      return <p className="mt-4 text-sm text-rose-600">{erpDetailError}</p>;
    }
    const items = erpDetailData?.items ?? [];
    if (!items.length) {
      return (
        <div className="mt-4 rounded-xl border border-dashed border-slate-200 bg-slate-50 p-4 text-sm text-slate-500">
          Nenhum detalhe disponivel para este grupo.
        </div>
      );
    }

    return (
      <div className="mt-4 rounded-2xl border border-slate-200 bg-white p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs uppercase tracking-[0.18em] text-slate-400">Detalhes do ERP</p>
            <h4 className="mt-1 text-base font-semibold text-slate-900">
              {erpMetricMeta.find((item) => item.key === selectedErpMetric)?.label || 'Detalhes'}
            </h4>
          </div>
          <Button variant="outline" onClick={() => setSelectedErpMetric(null)}>
            Fechar detalhes
          </Button>
        </div>
        <div className="mt-4 space-y-3">
          {items.map((item, index) => (
            <div key={`${selectedErpMetric}-${index}`} className="rounded-xl border border-slate-200 bg-slate-50 p-3">
              <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-3">
                {Object.entries(item).map(([key, value]) => (
                  <div key={key}>
                    <p className="text-xs uppercase text-slate-400">{key.replace(/([A-Z])/g, ' $1').trim()}</p>
                    <p className="mt-1 break-all text-sm font-medium text-slate-800">{formatValue(value)}</p>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  };

  return (
    <div className="flex flex-col gap-4">
      {activeOperation ? (
        <div className="reconciliation-overlay">
          <div className="reconciliation-overlay__card">
            <div className="reconciliation-overlay__spinner" />
            <p className="reconciliation-overlay__eyebrow">Importacao em andamento</p>
            <h2 className="reconciliation-overlay__title">{activeOperation.title}</h2>
            <p className="reconciliation-overlay__meta">{activeOperation.detail}</p>
            {blockingPercent !== null ? (
              <>
                <div className="reconciliation-overlay__bar">
                  <div
                    className="reconciliation-overlay__bar-fill"
                    style={{ width: `${Math.max(0, Math.min(100, blockingPercent))}%` }}
                  />
                </div>
                <div className="reconciliation-overlay__stats">
                  <span>{Math.round(blockingPercent)}%</span>
                  <span>{blockingMessage || 'Processando...'}</span>
                </div>
              </>
            ) : (
              <p className="reconciliation-overlay__meta">{blockingMessage || 'Processando...'}</p>
            )}
            <p className="reconciliation-overlay__hint">
              A tela foi bloqueada temporariamente para evitar novas acoes durante a importacao.
            </p>
          </div>
        </div>
      ) : null}

      <header>
        <h1 className="text-2xl font-semibold text-slate-900">Importar arquivos</h1>
        <p className="text-sm text-slate-500">
          Central operacional para entrada de arquivos do ERP e das adquirentes, com upload manual,
          varredura e sincronizacao SFTP.
        </p>
      </header>

      <div className="grid gap-4 xl:grid-cols-2">
      <Card className="p-4">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div title="Recomendado quando o operador precisa subir uma planilha do ERP ou confirmar se a pasta de inbox possui arquivos pendentes.">
            <p className="text-xs uppercase tracking-[0.18em] text-slate-400">1. ERP</p>
            <h2 className="mt-1 text-lg font-semibold text-slate-900">Importacao Interdata</h2>
          </div>
          <div className="rounded-full border border-slate-200 bg-slate-50 px-3 py-2 text-xs font-semibold text-slate-600">
            ERP - INTERDATA
          </div>
        </div>

        <div className="mt-4">
          <div
            className="rounded-2xl border border-slate-200 bg-white p-4"
            title="Arquivos colocados na pasta de entrada do ERP sao importados automaticamente. Use o upload quando o arquivo estiver no computador do operador."
          >
            <p className="text-xs uppercase text-slate-400">Acoes disponiveis</p>
            <div className="mt-3 flex flex-wrap items-center gap-2">
              <label
                className="inline-flex cursor-pointer items-center gap-2 rounded-full border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700"
                title="Arquivos colocados na pasta de entrada do ERP sao importados automaticamente. Use o upload quando o arquivo estiver no computador do operador."
              >
                Enviar arquivo ERP
                <input
                  type="file"
                  className="hidden"
                  onChange={(event) => {
                    const file = event.target.files?.[0];
                    if (file) {
                      const uploadId = createUploadId();
                      interdataUploadMutation.mutate({ file, uploadId });
                    }
                    event.currentTarget.value = '';
                  }}
                />
              </label>
            </div>
            {(interdataScanMutation.isPending || interdataUploadMutation.isPending) && <Spinner />}
            {interdataUploadMutation.isPending && (uploadProgress !== null || processingProgress !== null) && (
              <div className="mt-4 rounded-2xl border border-slate-200 bg-slate-50 p-4">
                <div className="flex items-center justify-between text-xs text-slate-500">
                  <span>{processingProgress !== null ? 'Processando arquivo...' : 'Enviando arquivo...'}</span>
                  <span>{(processingProgress ?? uploadProgress ?? 0).toFixed(0)}%</span>
                </div>
                {processingMessage ? <p className="mt-1 text-xs text-slate-400">{processingMessage}</p> : null}
                <div className="mt-2 h-2 w-full overflow-hidden rounded-full bg-slate-200">
                  <div
                    className="h-full bg-emerald-500 transition-all"
                    style={{ width: `${processingProgress ?? uploadProgress ?? 0}%` }}
                  />
                </div>
              </div>
            )}
            {(interdataScanMutation.isError || interdataUploadMutation.isError) && (
              <p className="mt-2 text-xs text-rose-600">
                {(interdataScanMutation.error as Error)?.message ||
                  (interdataUploadMutation.error as Error)?.message ||
                  'Falha ao processar a importacao do ERP.'}
              </p>
            )}
          </div>
        </div>
      </Card>

      <Card className="p-4">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div title="Escolha entre sincronizar arquivos do servidor SFTP ou enviar manualmente um arquivo isolado.">
            <p className="text-xs uppercase tracking-[0.18em] text-slate-400">2. Adquirentes</p>
            <h2 className="mt-1 text-lg font-semibold text-slate-900">Importacao de Cielo, Sipag e Sicredi</h2>
          </div>
          <div className="rounded-full border border-slate-200 bg-slate-50 px-3 py-2 text-xs font-semibold text-slate-600">
            {acquirerNames}
          </div>
        </div>

        <div className="mt-4 grid gap-4 lg:grid-cols-[1.1fr_0.9fr]">
          <div
            className="rounded-2xl border border-slate-200 bg-slate-50 p-4"
            title="Use esta acao para baixar automaticamente os arquivos das adquirentes marcadas abaixo."
          >
            <p className="text-xs uppercase text-slate-400">Sincronizacao server-SFTP</p>
            <div className="mt-4 flex flex-wrap items-center gap-3">
              <label className="inline-flex items-center gap-2 text-sm text-slate-700">
                <input type="checkbox" checked={remoteTargets.cielo} disabled readOnly />
                CIELO
              </label>
              <label className="inline-flex items-center gap-2 text-sm text-slate-700">
                <input type="checkbox" checked={remoteTargets.sipag} disabled readOnly />
                SIPAG
              </label>
              <label className="inline-flex items-center gap-2 text-sm text-slate-700">
                <input type="checkbox" checked={remoteTargets.sicredi} disabled readOnly />
                SICREDI
              </label>
            </div>
            <div className="mt-4 flex flex-wrap items-center gap-3">
              <Button
                title="Use esta acao para baixar automaticamente os arquivos das adquirentes marcadas abaixo."
                onClick={() => {
                  setRemoteResult(null);
                  remotePullMutation.mutate({
                    cielo: remoteTargets.cielo,
                    sipag: remoteTargets.sipag,
                    sicredi: remoteTargets.sicredi,
                  });
                }}
                disabled={remotePullMutation.isPending}
              >
                {remotePullMutation.isPending ? 'Sincronizando...' : 'Sincronizar arquivos remotos'}
              </Button>
              <p className="text-xs text-slate-500">
                Selecionados: {selectedAcquirers.join(', ') || 'nenhum'}
              </p>
            </div>
            {remotePullMutation.isPending ? <Spinner /> : null}
            {remoteError ? <p className="mt-2 text-xs text-rose-600">{remoteError}</p> : null}
          </div>

          <div
            className="rounded-2xl border border-slate-200 bg-white p-4"
            title="Indicado para importar um arquivo avulso sem depender do server-SFTP."
          >
            <p className="text-xs uppercase text-slate-400">Upload manual</p>
            <div className="mt-4 flex flex-wrap items-center gap-2">
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
              <label
                className="inline-flex cursor-pointer items-center gap-2 rounded-full border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700"
                title="Formatos permitidos: .csv, .json e .txt, conforme a adquirente selecionada."
              >
                Enviar arquivo
                <input
                  type="file"
                  accept=".csv,.json,.txt"
                  className="hidden"
                  onChange={(event) => {
                    const file = event.target.files?.[0];
                    if (file) {
                      acquirerUploadMutation.mutate({ file, acquirer });
                    }
                    event.currentTarget.value = '';
                  }}
                />
              </label>
            </div>
            {acquirerUploadMutation.isPending && <Spinner />}
            {acquirerUploadMutation.isError ? (
              <p className="mt-2 text-xs text-rose-600">
                {(acquirerUploadMutation.error as Error)?.message || 'Falha ao importar arquivo.'}
              </p>
            ) : null}
          </div>
        </div>

      </Card>
      </div>

      {hasRecentResults ? (
        <Card className="p-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <p className="text-xs uppercase tracking-[0.18em] text-slate-400">Resultados recentes</p>
              <h2 className="mt-1 text-lg font-semibold text-slate-900">Resumo das importacoes</h2>
            </div>
          </div>

          <div className="mt-4 grid gap-4 xl:grid-cols-3">
            {lastInterdataResult ? (
              <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <p className="text-xs uppercase tracking-[0.18em] text-slate-400">ERP</p>
                    <h3 className="mt-1 text-base font-semibold text-slate-900">Importacao Interdata</h3>
                  </div>
                  <div className="rounded-full bg-white px-3 py-1 text-xs font-semibold text-slate-700">
                    {lastInterdataResult.alreadyImported ? 'Ja importado' : 'Atualizado'}
                  </div>
                </div>
                {lastInterdataResult.alreadyImported ? (
                  <div className="mt-4 rounded-xl border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800">
                    <p className="font-semibold">Arquivo ja importado anteriormente</p>
                    <p className="mt-1 break-all">{lastInterdataResult.message || 'O arquivo enviado ja existe no historico de importacao.'}</p>
                  </div>
                ) : null}
                <div className="mt-4 grid grid-cols-2 gap-2">
                  {erpMetricMeta.map((item) => (
                    <button
                      key={item.key}
                      type="button"
                      onClick={() => erpDetailsMutation.mutate(item.key)}
                      className={`rounded-xl p-3 text-left transition ${
                        selectedErpMetric === item.key
                          ? 'bg-blue-50 ring-1 ring-blue-300'
                          : 'bg-white hover:bg-slate-50'
                      }`}
                    >
                      <p className="text-panel-muted text-xs uppercase">{item.label}</p>
                      <p className="text-panel-strong mt-1 text-lg font-bold">{item.value}</p>
                    </button>
                  ))}
                </div>
                {selectedErpMetric ? renderErpDetailContent() : null}
              </div>
            ) : null}

            {lastAcquirerResult ? (
              <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <p className="text-xs uppercase tracking-[0.18em] text-slate-400">Adquirente</p>
                    <h3 className="mt-1 text-base font-semibold text-slate-900">Upload manual</h3>
                  </div>
                  <div className="rounded-full bg-white px-3 py-1 text-xs font-semibold text-slate-700">
                    {lastAcquirerResult.alreadyImported
                      ? `${lastAcquirerResult.acquirer} • Ja importado`
                      : `${lastAcquirerResult.acquirer} • ${lastAcquirerResult.format}`}
                  </div>
                </div>
                {lastAcquirerResult.alreadyImported ? (
                  <div className="mt-4 rounded-xl border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800">
                    <p className="font-semibold">Arquivo ja importado anteriormente</p>
                    <p className="mt-1 break-all">{lastAcquirerResult.message || 'O arquivo enviado ja existe no historico de importacao.'}</p>
                  </div>
                ) : null}
                <div className="mt-4 grid grid-cols-3 gap-2">
                  <div className="rounded-xl bg-white p-3">
                    <p className="text-panel-muted text-xs uppercase">Inseridos</p>
                    <p className="text-panel-strong mt-1 text-lg font-bold">{lastAcquirerResult.inserted}</p>
                  </div>
                  <div className="rounded-xl bg-white p-3">
                    <p className="text-panel-muted text-xs uppercase">Duplicados</p>
                    <p className="text-panel-strong mt-1 text-lg font-bold">{lastAcquirerResult.duplicates}</p>
                  </div>
                  <div className="rounded-xl bg-white p-3">
                    <p className="text-panel-muted text-xs uppercase">Invalidos</p>
                    <p className="text-panel-strong mt-1 text-lg font-bold">{lastAcquirerResult.invalidRows}</p>
                  </div>
                </div>
              </div>
            ) : null}

            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <p className="text-xs uppercase tracking-[0.18em] text-slate-400">Adquirentes</p>
                  <h3 className="mt-1 text-base font-semibold text-slate-900">Sincronizacao SFTP</h3>
                </div>
                <div className="rounded-full bg-white px-3 py-1 text-xs font-semibold text-slate-700">
                  {remotePullData ? 'Atualizado' : 'Sem leitura'}
                </div>
              </div>
              {remotePullData ? (
                <div className="mt-4 grid grid-cols-2 gap-2">
                  <div className="rounded-xl bg-white p-3">
                    <p className="text-panel-muted text-xs uppercase">Encontrados</p>
                    <p className="text-panel-strong mt-1 text-lg font-bold">{remotePullData.summary.total.listed}</p>
                  </div>
                  <div className="rounded-xl bg-white p-3">
                    <p className="text-panel-muted text-xs uppercase">Baixados</p>
                    <p className="text-panel-strong mt-1 text-lg font-bold">{remotePullData.summary.total.downloaded}</p>
                  </div>
                  <div className="rounded-xl bg-white p-3">
                    <p className="text-panel-muted text-xs uppercase">Processed</p>
                    <p className="text-panel-strong mt-1 text-lg font-bold">{remotePullData.summary.total.movedProcessed}</p>
                  </div>
                  <div className="rounded-xl bg-white p-3">
                    <p className="text-panel-muted text-xs uppercase">Ignorados</p>
                    <p className="text-panel-strong mt-1 text-lg font-bold">{remotePullData.summary.total.ignored}</p>
                  </div>
                  <div className="rounded-xl bg-white p-3">
                    <p className="text-panel-muted text-xs uppercase">Movidos error</p>
                    <p className="text-panel-strong mt-1 text-lg font-bold">{remotePullData.summary.total.movedError}</p>
                  </div>
                  <div className="rounded-xl bg-white p-3">
                    <p className="text-panel-muted text-xs uppercase">Duracao</p>
                    <p className="text-panel-strong mt-1 text-lg font-bold">
                      {formatDuration(remotePullData.startedAt, remotePullData.finishedAt)}
                    </p>
                  </div>
                </div>
              ) : (
                <div className="mt-4">
                  <EmptyState title="Sem sincronizacao" description="Execute a sincronizacao remota para gerar as contagens." />
                </div>
              )}
            </div>
          </div>
        </Card>
      ) : null}

      {remotePullData ? (
        <Card className="p-4">
          <div className="space-y-4">
            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <p className="text-xs uppercase tracking-[0.18em] text-slate-400">Resultado da sincronizacao</p>
                  <h3 className="mt-1 text-lg font-semibold text-slate-900">
                    {remotePullData.ok ? 'Sincronizacao concluida' : 'Sincronizacao com alertas'}
                  </h3>
                  <p className="mt-1 text-sm text-slate-500">
                    Host {remotePullData.host} • alvo {selectedAcquirers.join(', ') || 'nenhum'}
                  </p>
                </div>
                <div
                  className={`rounded-full px-3 py-1 text-xs font-semibold ${
                    remotePullData.ok ? 'bg-emerald-100 text-emerald-700' : 'bg-amber-100 text-amber-700'
                  }`}
                >
                  {remotePullData.ok ? 'OK' : 'ATENCAO'}
                </div>
              </div>

              <div className="mt-4 grid gap-3 md:grid-cols-4">
                <div className="rounded-xl border border-slate-200 bg-white p-3">
                  <p className="text-panel-muted text-xs uppercase">Inicio</p>
                  <p className="text-panel-strong mt-1 text-sm font-semibold">{formatDateTime(remotePullData.startedAt)}</p>
                </div>
                <div className="rounded-xl border border-slate-200 bg-white p-3">
                  <p className="text-panel-muted text-xs uppercase">Fim</p>
                  <p className="text-panel-strong mt-1 text-sm font-semibold">{formatDateTime(remotePullData.finishedAt)}</p>
                </div>
                <div className="rounded-xl border border-slate-200 bg-white p-3">
                  <p className="text-panel-muted text-xs uppercase">Duracao</p>
                  <p className="text-panel-strong mt-1 text-sm font-semibold">
                    {formatDuration(remotePullData.startedAt, remotePullData.finishedAt)}
                  </p>
                </div>
                <div className="rounded-xl border border-slate-200 bg-white p-3">
                  <p className="text-panel-muted text-xs uppercase">Baixados</p>
                  <p className="text-panel-strong mt-1 text-sm font-semibold">{remotePullData.summary.total.downloaded}</p>
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
                        <h4 className="mt-1 text-base font-semibold text-slate-900">{acquirerLabels[providerKey]}</h4>
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
                      <p className="mt-3 text-xs text-slate-500">Arquivos recentes pulados: {provider.skippedRecent}</p>
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
                Total: {remotePullData.summary.total.listed} encontrados, {remotePullData.summary.total.downloaded}{' '}
                baixados, {remotePullData.summary.total.movedProcessed} movidos para processed.
              </p>
            </div>

            {showRemoteRaw ? (
              <pre className="overflow-auto rounded-2xl border border-slate-200 bg-slate-950 p-4 text-xs text-slate-100">
                {JSON.stringify(remoteResult, null, 2)}
              </pre>
            ) : null}
          </div>
        </Card>
      ) : null}
    </div>
  );
};
