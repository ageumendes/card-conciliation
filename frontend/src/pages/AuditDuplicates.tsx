import { Fragment, useMemo, useState } from 'react';
import { Card } from '../components/common/Card';
import { Button } from '../components/common/Button';
import { Input } from '../components/common/Input';
import { Select } from '../components/common/Select';
import { EmptyState } from '../components/common/EmptyState';
import { Spinner } from '../components/common/Spinner';
import { FlagsCell } from '../components/FlagsCell';
import { getAuditDuplicates } from '../api/reconciliation';
import { AuditDuplicateRow } from '../api/types';
import { formatCurrency } from '../utils/format';
import { SourceBadgesCell } from '../components/SourceBadgesCell';
import styles from '../pages/Reconciliation.module.css';

const formatDateInput = (date: Date) => {
  const pad = (value: number) => String(value).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
};

const formatDateDisplay = (value?: string | null) => {
  if (!value) {
    return '--';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  const pad = (num: number) => String(num).padStart(2, '0');
  return `${pad(date.getDate())}/${pad(date.getMonth() + 1)}/${date.getFullYear()}`;
};

export const AuditDuplicates = () => {
  const today = new Date();
  const yesterday = new Date(today.getFullYear(), today.getMonth(), today.getDate() - 1);

  const [acquirer, setAcquirer] = useState<'all' | 'cielo' | 'sipag' | 'sicredi'>('cielo');
  const [from, setFrom] = useState(formatDateInput(yesterday));
  const [to, setTo] = useState(formatDateInput(today));
  const [onlySuspicious, setOnlySuspicious] = useState(true);
  const [rows, setRows] = useState<AuditDuplicateRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedKey, setExpandedKey] = useState<string | null>(null);

  const normalizedRows = useMemo(
    () =>
      rows.map((row) => ({
        ...row,
        erpIds: Array.isArray(row.erpIds) ? row.erpIds : [],
        acqIds: Array.isArray(row.acqIds) ? row.acqIds : [],
        reconIds: Array.isArray(row.reconIds) ? row.reconIds : [],
      })),
    [rows],
  );

  const handleLoad = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await getAuditDuplicates({
        acquirer,
        from: from || undefined,
        to: to || undefined,
        onlySuspicious,
      });
      setRows(data?.data ?? []);
    } catch (err) {
      const message = (err as Error)?.message ?? 'Falha ao carregar audit duplicates.';
      setError(message);
    } finally {
      setLoading(false);
    }
  };

  const handleClear = () => {
    setRows([]);
    setError(null);
    setExpandedKey(null);
  };

  return (
    <div className="mx-auto flex w-full max-w-[1400px] flex-col gap-1 px-6 py-1">
      <div>
        <h1 className="text-2xl font-semibold text-slate-900">Auditoria de Duplicidades</h1>
        <p className="text-sm text-slate-500">
          Compare ERP, adquirentes e conciliados por data, metodo e valor.
        </p>
      </div>

      <Card className="p-1">
        <div className="grid grid-cols-1 gap-1 md:grid-cols-6">
          <div className="md:col-span-2">
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              Adquirente
            </label>
            <Select value={acquirer} onChange={(event) => setAcquirer(event.target.value as any)}>
              <option value="all">ALL</option>
              <option value="cielo">CIELO</option>
              <option value="sipag">SIPAG</option>
              <option value="sicredi">SICREDI</option>
            </Select>
          </div>
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">De</label>
            <Input type="date" value={from} onChange={(event) => setFrom(event.target.value)} />
          </div>
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">Ate</label>
            <Input type="date" value={to} onChange={(event) => setTo(event.target.value)} />
          </div>
          <div className="flex items-end gap-2">
            <label className="inline-flex items-center gap-2 text-sm text-slate-600">
              <input
                type="checkbox"
                className="h-4 w-4 rounded border-slate-300 text-slate-900"
                checked={onlySuspicious}
                onChange={(event) => setOnlySuspicious(event.target.checked)}
              />
              Somente suspeitos
            </label>
          </div>
          <div className="flex items-end justify-end gap-2 md:col-span-1">
            <Button variant="ghost" onClick={handleClear}>
              Limpar
            </Button>
            <Button onClick={handleLoad} disabled={loading}>
              {loading ? 'Carregando...' : 'Carregar'}
            </Button>
          </div>
        </div>
      </Card>

      <section className={styles.tableBody}>
        <div className={styles.tableShell}>
          <div className={styles.tableCard}>
            {loading ? (
              <div className="flex min-h-[200px] items-center justify-center">
                <Spinner />
              </div>
            ) : error ? (
              <div className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-6 text-sm text-rose-700">
                {error}
              </div>
            ) : !normalizedRows.length ? (
              <EmptyState
                title="Sem dados carregados"
                description="Defina filtros e clique em Carregar para ver resultados."
              />
            ) : (
              <div className="max-h-[60vh] overflow-y-auto overflow-x-hidden scrollbar-thin">
                <table className="w-full table-auto border-separate border-spacing-0 text-left text-sm">
                  <thead className="sticky top-0 z-10 bg-slate-50">
                    <tr>
                      {[
                        'Data',
                        'Metodo',
                        'Valor',
                        'ERP',
                        'Adquirente',
                        'Conciliado',
                        'Origem',
                        'Flags',
                        'IDs',
                      ].map((header) => (
                        <th
                          key={header}
                          className="border-b border-slate-200 px-4 text-xs font-semibold uppercase tracking-wide text-slate-500"
                        >
                          {header}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {normalizedRows.map((row) => {
                      const key = `${row.key.saleDate ?? 'na'}-${row.key.methodGroup ?? 'na'}-${row.key.grossAmount ?? 'na'}`;
                      const isExpanded = expandedKey === key;
                      const hasMismatch = row.flags.mismatch;
                      return (
                        <Fragment key={key}>
                          <tr className={hasMismatch ? 'bg-amber-50/60' : 'bg-white'}>
                            <td className="border-b border-slate-100 px-4 text-slate-700">
                              {formatDateDisplay(row.key.saleDate)}
                            </td>
                            <td className="border-b border-slate-100 px-4 text-slate-700">
                              {row.key.methodGroup ?? '--'}
                            </td>
                            <td className="border-b border-slate-100 px-4 text-slate-700">
                              {formatCurrency(row.key.grossAmount ?? null)}
                            </td>
                            <td className="border-b border-slate-100 px-4 text-slate-700">
                              {row.erpCount}
                            </td>
                            <td className="border-b border-slate-100 px-4 text-slate-700">
                              {row.acqCount}
                            </td>
                            <td className="border-b border-slate-100 px-4 text-slate-700">
                              {row.reconCount}
                            </td>
                            <td className="border-b border-slate-100 px-4">
                              <SourceBadgesCell
                                erpCount={row.erpCount}
                                acqCount={row.acqCount}
                                reconCount={row.reconCount}
                              />
                            </td>
                            <td className="border-b border-slate-100 px-4">
                              <FlagsCell flags={row.flags} />
                            </td>
                            <td className="border-b border-slate-100 px-4">
                              <Button
                                variant="ghost"
                                className="px-2 py-1 text-xs"
                                onClick={() => setExpandedKey(isExpanded ? null : key)}
                              >
                                {isExpanded ? 'Ocultar' : 'Ver'}
                              </Button>
                            </td>
                          </tr>
                          {isExpanded ? (
                            <tr className="bg-slate-50">
                              <td colSpan={8} className="border-b border-slate-200 px-4 py-4 text-sm text-slate-700">
                                <div className="grid gap-2 md:grid-cols-3">
                                  <div>
                                    <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                                      ERP IDs
                                    </p>
                                    <p className="text-sm text-slate-700">
                                      {row.erpIds.length ? row.erpIds.join(', ') : '--'}
                                    </p>
                                  </div>
                                  <div>
                                    <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                                      ACQ IDs
                                    </p>
                                    <p className="text-sm text-slate-700">
                                      {row.acqIds.length ? row.acqIds.join(', ') : '--'}
                                    </p>
                                  </div>
                                  <div>
                                    <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                                      Recon IDs
                                    </p>
                                    <p className="text-sm text-slate-700">
                                      {row.reconIds.length ? row.reconIds.join(', ') : '--'}
                                    </p>
                                  </div>
                                </div>
                              </td>
                            </tr>
                          ) : null}
                        </Fragment>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      </section>
    </div>
  );
};
