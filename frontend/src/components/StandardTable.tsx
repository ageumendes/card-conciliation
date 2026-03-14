import { Fragment, type ReactNode } from 'react';
import clsx from 'clsx';
import { EmptyState } from './common/EmptyState';
import { Spinner } from './common/Spinner';
import { StandardRow } from '../modules/reconciliation/types';
import { AcquirerLogo } from './AcquirerLogo';
import { BrandLogo } from './BrandLogo';
import { StatusBadge } from './StatusBadge';

interface StandardTableProps {
  rows: StandardRow[];
  loading?: boolean;
  emptyText?: string;
  onRowClick?: (row: StandardRow) => void;
  selectedRowId?: number | null;
  showOrigin?: boolean;
  showMatch?: boolean;
  enableCellTitles?: boolean;
  wrapperClassName?: string;
  tableClassName?: string;
  enableHorizontalScroll?: boolean;
  scrollWrapperClassName?: string;
  rowClassName?: string;
  selectedRowClassName?: string;
  expandedRowClassName?: string;
  sortBy?: 'datetime' | 'amount' | null;
  sortDir?: 'asc' | 'desc' | null;
  onSortChange?: (next: { by: 'datetime' | 'amount' | null; dir: 'asc' | 'desc' | null }) => void;
  expandedRowId?: number | null;
  renderExpandedRow?: (row: StandardRow) => ReactNode;
  hideFeesAndNet?: boolean;
}

const getHeaders = (showOrigin: boolean, showMatch: boolean, hideFeesAndNet: boolean) => {
  const headers = [
    'Data/Hora',
    'Adquirente',
    'Valor Total',
    'Forma Pgto',
    'Bandeira',
    'Status',
  ];
  if (!hideFeesAndNet) {
    headers.splice(3, 0, 'Taxa', 'Valor Líquido');
  }
  if (showMatch) {
    headers.push('Camada');
  }
  if (showOrigin) {
    headers.push('Origem');
  }
  headers.push('Nª Venda', 'NSU');
  return headers;
};

export const StandardTable = ({
  rows,
  loading,
  emptyText,
  onRowClick,
  selectedRowId,
  showOrigin = false,
  showMatch = false,
  enableCellTitles = false,
  wrapperClassName,
  tableClassName,
  enableHorizontalScroll = false,
  scrollWrapperClassName,
  rowClassName,
  selectedRowClassName,
  expandedRowClassName,
  sortBy = null,
  sortDir = null,
  onSortChange,
  expandedRowId,
  renderExpandedRow,
  hideFeesAndNet = false,
}: StandardTableProps) => {
  const headers = getHeaders(showOrigin, showMatch, hideFeesAndNet);
  const content = loading ? (
    <div className="flex min-h-full items-center justify-center py-10">
      <Spinner />
    </div>
  ) : !rows.length ? (
    <div className="flex min-h-full items-center justify-center px-4 py-10">
      <EmptyState
        title={emptyText || 'Nenhum registro encontrado'}
        description="Ajuste os filtros ou recarregue a tabela."
      />
    </div>
  ) : (
    <table
      className={clsx(
        'min-w-full table-auto border-separate border-spacing-0 text-left text-sm',
        tableClassName,
      )}
    >
      <thead className="sticky top-0 z-10 bg-slate-50">
        <tr>
          {headers.map((header) => {
            const sortKey =
              header === 'Data/Hora' ? 'datetime' : header === 'Valor Total' ? 'amount' : null;
            const isSortable = Boolean(sortKey && onSortChange);
            const isActive = sortKey && sortBy === sortKey;
            const indicator = isActive ? (sortDir === 'asc' ? '▲' : '▼') : '';
            return (
              <th
                key={header}
                className={clsx(
                  'border-b border-slate-200 px-4 text-xs font-semibold uppercase tracking-wide text-slate-500',
                  isSortable && 'cursor-pointer select-none',
                )}
                onClick={() => {
                  if (!sortKey || !onSortChange) {
                    return;
                  }
                  const defaultDir = sortKey === 'amount' ? 'asc' : 'desc';
                  let nextBy: 'datetime' | 'amount' | null = sortKey;
                  let nextDir: 'asc' | 'desc' | null = defaultDir;
                  if (sortBy === sortKey) {
                    if (sortDir === 'desc') {
                      nextDir = 'asc';
                    } else if (sortDir === 'asc') {
                      nextBy = null;
                      nextDir = null;
                    } else {
                      nextDir = defaultDir;
                    }
                  }
                  onSortChange({ by: nextBy, dir: nextDir });
                }}
              >
                <span className="inline-flex min-w-0 items-center gap-1">
                  <span className="min-w-0 truncate">{header}</span>
                  {indicator ? <span className="text-[10px]">{indicator}</span> : null}
                </span>
              </th>
            );
          })}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, idx) => {
          const isSelected =
            selectedRowId !== null && selectedRowId !== undefined && row.rowId === selectedRowId;
          const isExpanded = expandedRowId !== null && expandedRowId !== undefined && row.rowId === expandedRowId;
          const showExpandIcon = Boolean(renderExpandedRow);
          const confidenceLabel =
            typeof row.matchConfidence === 'number'
              ? row.matchConfidence >= 90
                ? 'Alto'
                : row.matchConfidence >= 75
                  ? 'Medio'
                  : 'Baixo'
              : '';
          const matchLabel = row.matchLayer ? `C${row.matchLayer}` : '--';
          const matchTitle = row.matchReason
            ? `${row.matchReason}${confidenceLabel ? ` (${confidenceLabel})` : ''}`
            : confidenceLabel || undefined;
          const resolvedSelectedClass =
            selectedRowClassName ?? 'bg-slate-300/60 outline outline-1 outline-slate-400';
          return (
            <Fragment key={`${row.saleNoText}-${row.nsuText}-${idx}`}>
              <tr
                onClick={() => onRowClick?.(row)}
                className={clsx(
                  'cursor-pointer transition',
                  'hover:bg-slate-100',
                  rowClassName,
                  isSelected && resolvedSelectedClass,
                )}
              >
                <td
                  className="border-b border-slate-100 px-4 text-slate-700"
                  title={enableCellTitles ? row.datetimeText : undefined}
                >
                  <div className="flex items-center gap-2">
                    {showExpandIcon ? (
                      <span className="text-xs text-slate-400">{isExpanded ? '▾' : '▸'}</span>
                    ) : null}
                    <span>{row.datetimeText}</span>
                  </div>
                </td>
            <td
              className="border-b border-slate-100 px-4 text-center text-slate-700"
              title={enableCellTitles ? row.operatorText : undefined}
            >
              <AcquirerLogo acquirer={row.operatorKey || row.operatorText} className="mx-auto" />
            </td>
            <td
              className="border-b border-slate-100 px-4 text-right text-slate-700"
              title={enableCellTitles ? row.totalText : undefined}
            >
              {row.totalText}
            </td>
            {!hideFeesAndNet ? (
              <td
                className="border-b border-slate-100 px-4 text-right text-slate-700"
                title={enableCellTitles ? row.feesText : undefined}
              >
                {row.feesText}
              </td>
            ) : null}
            {!hideFeesAndNet ? (
              <td
                className="border-b border-slate-100 px-4 text-right text-slate-700"
                title={enableCellTitles ? row.netText : undefined}
              >
                {row.netText}
              </td>
            ) : null}
            <td
              className="border-b border-slate-100 px-4 text-slate-700"
              title={enableCellTitles ? row.paymentText : undefined}
            >
              {row.paymentText}
            </td>
            <td
              className="border-b border-slate-100 px-4 text-slate-700"
              title={enableCellTitles ? row.brandText : undefined}
            >
              <div className="flex items-center justify-center">
                <BrandLogo brand={row.brandText} />
              </div>
            </td>
            <td
              className="border-b border-slate-100 px-4 text-slate-700"
              title={enableCellTitles ? row.statusText : undefined}
            >
              <StatusBadge status={row.statusText} />
            </td>
            {showMatch ? (
              <td
                className="border-b border-slate-100 px-4 text-xs text-slate-700"
                title={matchTitle}
              >
                <span className="rounded-full bg-slate-100 px-2 py-1 uppercase">
                  {matchLabel}
                </span>
              </td>
            ) : null}
            {showOrigin ? (
              <td
                className="border-b border-slate-100 px-4 text-xs text-slate-600"
                title={enableCellTitles ? row.originText || 'AUTO' : undefined}
              >
                <span className="rounded-full bg-slate-100 px-2 py-1 uppercase">
                  {row.originText || 'AUTO'}
                </span>
              </td>
            ) : null}
            <td
              className="border-b border-slate-100 px-4 text-slate-700"
              title={enableCellTitles ? row.saleNoText : undefined}
            >
              {row.saleNoText}
            </td>
            <td
              className="border-b border-slate-100 px-4 text-slate-700"
              title={enableCellTitles ? row.nsuText.replace(/\s*\n\s*/g, ' | ') : undefined}
            >
              {row.nsuText.replace(/\s*\n\s*/g, ' | ')}
            </td>
              </tr>
              {renderExpandedRow && isExpanded ? (
                <tr className={clsx('bg-slate-50', expandedRowClassName)}>
                  <td className="border-b border-slate-100 px-4 py-0" colSpan={headers.length}>
                    {renderExpandedRow(row)}
                  </td>
                </tr>
              ) : null}
            </Fragment>
          );
        })}
      </tbody>
    </table>
  );

  const tableContent = enableHorizontalScroll ? (
    <div className={clsx('overflow-x-auto', scrollWrapperClassName)}>{content}</div>
  ) : (
    content
  );

  return (
    <div
      className={clsx(
        enableHorizontalScroll
          ? 'overflow-y-auto overflow-x-auto rounded-2xl border border-slate-200 bg-white shadow-card'
          : 'overflow-y-auto overflow-x-hidden rounded-2xl border border-slate-200 bg-white shadow-card',
        wrapperClassName,
      )}
    >
      {tableContent}
    </div>
  );
};
