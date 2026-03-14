type StatusBadgeProps = {
  status?: string | null;
  className?: string;
};

const normalizeStatus = (status?: string | null) => {
  if (!status) return '';
  return status
    .trim()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();
};

const getStatusTone = (normalized: string) => {
  if (normalized.includes('sem concili')) {
    return 'border border-amber-300 bg-amber-200/70 text-amber-900';
  }
  if (normalized.includes('aprov') || normalized.includes('conciliad')) {
    return 'border border-emerald-300 bg-emerald-200/70 text-emerald-900';
  }
  if (
    normalized.includes('cancel') ||
    normalized.includes('estorn') ||
    normalized.includes('negad') ||
    normalized.includes('reprov')
  ) {
    return 'border border-rose-300 bg-rose-200/70 text-rose-900';
  }
  if (normalized.includes('pend') || normalized.includes('nao')) {
    return 'border border-amber-300 bg-amber-200/70 text-amber-900';
  }
  return 'border border-slate-300 bg-slate-200/70 text-slate-700';
};

export const StatusBadge = ({ status, className }: StatusBadgeProps) => {
  const text = status?.toString().trim() || '—';
  const normalized = normalizeStatus(text);
  const tone = getStatusTone(normalized);

  return (
    <span
      className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-semibold ${tone} ${className ?? ''}`}
      title={text}
    >
      {text}
    </span>
  );
};
