import { useMutation, useQuery } from '@tanstack/react-query';
import { useRef, useState } from 'react';
import { fetchInterdataFiles, fetchInterdataPing, scanInterdata, uploadInterdata } from '../api/interdata';
import { Card } from '../components/common/Card';
import { Button } from '../components/common/Button';
import { Spinner } from '../components/common/Spinner';
import { EmptyState } from '../components/common/EmptyState';
import { queryClient } from '../app/queryClient';

export const InterdataImports = () => {
  const pingQuery = useQuery({ queryKey: ['interdata-ping'], queryFn: fetchInterdataPing });
  const filesQuery = useQuery({ queryKey: ['interdata-files'], queryFn: fetchInterdataFiles });
  const [uploadProgress, setUploadProgress] = useState<number | null>(null);
  const [processingProgress, setProcessingProgress] = useState<number | null>(null);
  const [processingMessage, setProcessingMessage] = useState<string | null>(null);
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

  const scanMutation = useMutation({
    mutationFn: scanInterdata,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['interdata-files'] }),
  });

  const uploadMutation = useMutation({
    mutationFn: (payload: { file: File; uploadId: string }) =>
      uploadInterdata(payload.file, { onProgress: setUploadProgress, uploadId: payload.uploadId }),
    onMutate: (payload) => {
      setUploadProgress(0);
      setProcessingProgress(0);
      setProcessingMessage(null);
      openProgress(payload.uploadId);
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['interdata-files'] }),
    onSettled: () => {
      setUploadProgress(null);
      setProcessingProgress(null);
      setProcessingMessage(null);
      closeProgress();
    },
  });

  return (
    <div className="flex flex-col gap-4">
      <header>
        <h1 className="text-2xl font-semibold text-slate-900">Importacoes Interdata</h1>
        <p className="text-sm text-slate-500">Uploads e varredura dos arquivos Excel.</p>
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
          <div className="mt-2 space-y-3 text-sm text-slate-600">
            <div className="flex items-center gap-2">
              <span
                className={`inline-flex h-2 w-2 rounded-full ${
                  pingQuery.data?.enabled ? 'bg-emerald-500' : 'bg-rose-500'
                }`}
              />
              <span className="font-semibold">
                {pingQuery.data?.enabled ? 'Importacao habilitada' : 'Importacao desabilitada'}
              </span>
            </div>
            <div>
              <p className="text-xs uppercase text-slate-400">Pastas</p>
              <ul className="mt-2 space-y-1 text-xs text-slate-500">
                <li>Entrada: {String((pingQuery.data as any)?.dirs?.dropDir ?? '-')}</li>
                <li>Arquivo: {String((pingQuery.data as any)?.dirs?.archiveDir ?? '-')}</li>
                <li>Erro: {String((pingQuery.data as any)?.dirs?.errorDir ?? '-')}</li>
              </ul>
            </div>
          </div>
        )}
      </Card>

      <Card className="p-4">
        <div className="flex flex-wrap items-center gap-2">
          <Button onClick={() => scanMutation.mutate()}>Scan</Button>
          <label className="inline-flex cursor-pointer items-center gap-2 rounded-full border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700">
            Upload
            <input
              type="file"
              className="hidden"
              onChange={(event) => {
                const file = event.target.files?.[0];
                if (file) {
                  const uploadId = createUploadId();
                  uploadMutation.mutate({ file, uploadId });
                }
                event.currentTarget.value = '';
              }}
            />
          </label>
        </div>
        {(scanMutation.isPending || uploadMutation.isPending) && <Spinner />}
        {uploadMutation.isPending && (uploadProgress !== null || processingProgress !== null) && (
          <div className="mt-3">
            <div className="flex items-center justify-between text-xs text-slate-500">
              <span>
                {processingProgress !== null ? 'Processando arquivo...' : 'Enviando arquivo...'}
              </span>
              <span>{(processingProgress ?? uploadProgress ?? 0).toFixed(0)}%</span>
            </div>
            {processingMessage ? (
              <p className="mt-1 text-xs text-slate-400">{processingMessage}</p>
            ) : null}
            <div className="mt-2 h-2 w-full overflow-hidden rounded-full bg-slate-200">
              <div
                className="h-full bg-emerald-500 transition-all"
                style={{ width: `${processingProgress ?? uploadProgress ?? 0}%` }}
              />
            </div>
          </div>
        )}
        {(scanMutation.isError || uploadMutation.isError) && (
          <p className="mt-2 text-xs text-rose-600">
            {(scanMutation.error as Error)?.message ||
              (uploadMutation.error as Error)?.message ||
              'Falha ao processar a importacao.'}
          </p>
        )}
      </Card>

      <Card className="p-4">
        <p className="text-xs uppercase text-slate-400">Arquivos no inbox</p>
        {filesQuery.isLoading ? (
          <Spinner />
        ) : filesQuery.data?.files?.length ? (
          <div className="mt-3 space-y-2">
            {filesQuery.data.files.map((file) => (
              <div key={file.filename} className="flex items-center justify-between text-sm text-slate-600">
                <span>{file.filename}</span>
                <span>{Math.round(file.size / 1024)} KB</span>
              </div>
            ))}
          </div>
        ) : (
          <EmptyState title="Nenhum arquivo encontrado" description="Envie um XLSX ou rode o scan." />
        )}
      </Card>
    </div>
  );
};
