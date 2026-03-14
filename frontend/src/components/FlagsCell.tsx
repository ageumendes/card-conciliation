type Flags = {
  erpDup: boolean;
  acqDup: boolean;
  reconDup: boolean;
  mismatch: boolean;
};

const DEFAULT_FLAGS: Flags = {
  erpDup: false,
  acqDup: false,
  reconDup: false,
  mismatch: false,
};

const FLAG_DEFINITIONS: Array<{
  key: keyof Flags;
  label: string;
  title: string;
}> = [
  {
    key: 'erpDup',
    label: 'ERP DUP',
    title:
      'ERP duplicado: existe mais de 1 registro no ERP (T_INTERDATA_SALES) para esta chave (data + método + valor). Possível importação duplicada ou duplicidade do PDV.',
  },
  {
    key: 'acqDup',
    label: 'ACQ DUP',
    title:
      'Adquirente duplicada: existe mais de 1 registro na adquirente (ex: T_CIELO_SALES/T_SIPAG_SALES/T_SICREDI_SALES) para esta chave. Possível reprocessamento de arquivo/EDI.',
  },
  {
    key: 'reconDup',
    label: 'RECON DUP',
    title:
      'Conciliado duplicado: existe mais de 1 registro na conciliação (T_RECONCILIATION) para esta chave. Isso é bug/duplicidade de match.',
  },
  {
    key: 'mismatch',
    label: 'MISMATCH',
    title:
      'Mismatch: os contadores entre ERP, Adquirente e Conciliado não batem (ex: ERP=1 ACQ=0). Indica venda sem par/pendente ou falha de import/conciliação.',
  },
];

const warnClass =
  'inline-flex items-center rounded-full border border-rose-300 bg-rose-200/60 px-2 py-0.5 text-[11px] font-semibold uppercase text-rose-900';
const neutralClass =
  'inline-flex items-center rounded-full border border-slate-300 bg-slate-200/70 px-2 py-0.5 text-[11px] font-semibold uppercase text-slate-700';

export const FlagsCell = ({ flags }: { flags?: Partial<Flags> | null }) => {
  const resolved = { ...DEFAULT_FLAGS, ...(flags ?? {}) };
  const activeFlags = FLAG_DEFINITIONS.filter((flag) => resolved[flag.key]);

  if (!activeFlags.length) {
    return (
      <span className={neutralClass} title="Sem flags de duplicidade ou mismatch.">
        OK
      </span>
    );
  }

  return (
    <div className="flex flex-wrap gap-2">
      {activeFlags.map((flag) => (
        <span key={flag.key} className={warnClass} title={flag.title}>
          {flag.label}
        </span>
      ))}
    </div>
  );
};

export type { Flags };
