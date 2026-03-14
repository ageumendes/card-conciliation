type SourceBadgesProps = {
  erpCount?: number | null;
  acqCount?: number | null;
  reconCount?: number | null;
};

const badgeClass =
  'inline-flex items-center rounded-full border border-slate-200 bg-slate-100 px-2 py-0.5 text-[11px] font-semibold uppercase text-slate-600';

export const SourceBadgesCell = ({ erpCount, acqCount, reconCount }: SourceBadgesProps) => {
  const erp = typeof erpCount === 'number' ? erpCount : 0;
  const acq = typeof acqCount === 'number' ? acqCount : 0;
  const recon = typeof reconCount === 'number' ? reconCount : 0;

  const items = [
    {
      key: 'ERP',
      show: erp > 0,
      title: 'Há registros no ERP (T_INTERDATA_SALES) para esta chave.',
    },
    {
      key: 'ADQ',
      show: acq > 0,
      title:
        'Há registros na adquirente (T_CIELO_SALES/T_SIPAG_SALES/T_SICREDI_SALES) para esta chave.',
    },
    {
      key: 'RECON',
      show: recon > 0,
      title: 'Há registros conciliados (T_RECONCILIATION) para esta chave.',
    },
  ];

  const visible = items.filter((item) => item.show);
  if (!visible.length) {
    return <span className="text-xs text-slate-500">-</span>;
  }

  return (
    <div className="flex flex-wrap gap-2">
      {visible.map((item) => (
        <span key={item.key} className={badgeClass} title={item.title}>
          {item.key}
        </span>
      ))}
    </div>
  );
};
