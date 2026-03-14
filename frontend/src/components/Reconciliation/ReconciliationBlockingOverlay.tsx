import { useQuery } from '@tanstack/react-query';
import { fetchReconciliationStatus } from '../../api/reconciliation';

const formatStartedAt = (value?: string | null) => {
  if (!value) {
    return '';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return '';
  }
  const pad = (num: number) => String(num).padStart(2, '0');
  return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
};

export const ReconciliationBlockingOverlay = () => {
  const statusQuery = useQuery({
    queryKey: ['reconciliation-runtime-status'],
    queryFn: fetchReconciliationStatus,
    refetchInterval: 2000,
    refetchIntervalInBackground: true,
    staleTime: 0,
  });

  const status = statusQuery.data?.data;
  if (!status?.running) {
    return null;
  }

  const total = Math.max(status.total || 0, 0);
  const processed = Math.max(status.processed || 0, 0);
  const percent = total > 0 ? Math.min(100, Math.round((processed / total) * 100)) : 0;

  return (
    <div className="reconciliation-overlay">
      <div className="reconciliation-overlay__card">
        <div className="reconciliation-overlay__spinner" />
        <p className="reconciliation-overlay__eyebrow">Conciliação em andamento</p>
        <h2 className="reconciliation-overlay__title">{status.message || 'Processando...'}</h2>
        <p className="reconciliation-overlay__meta">
          Início: {formatStartedAt(status.startedAt)} {status.dryRun ? '• Dry-run' : ''}
        </p>
        <div className="reconciliation-overlay__bar">
          <div
            className="reconciliation-overlay__bar-fill"
            style={{ width: `${percent}%` }}
          />
        </div>
        <div className="reconciliation-overlay__stats">
          <span>{processed}/{total || '--'} processadas</span>
          <span>{status.matched} conciliadas</span>
          <span>{status.pending} pendentes</span>
          <span>{status.errors} erros</span>
        </div>
        <p className="reconciliation-overlay__hint">
          A interface foi bloqueada para evitar inconsistências enquanto a conciliação automatica roda.
        </p>
      </div>
    </div>
  );
};
