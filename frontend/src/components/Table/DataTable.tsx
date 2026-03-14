import {
  ColumnDef,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  SortingState,
  useReactTable,
} from '@tanstack/react-table';
import { useMemo, useState } from 'react';
import clsx from 'clsx';
import { Spinner } from '../common/Spinner';
import { EmptyState } from '../common/EmptyState';

interface DataTableProps<T> {
  data: T[];
  columns: ColumnDef<T, any>[];
  isLoading?: boolean;
  onRowClick?: (row: T) => void;
}

export const DataTable = <T extends object>({
  data,
  columns,
  isLoading,
  onRowClick,
}: DataTableProps<T>) => {
  const [sorting, setSorting] = useState<SortingState>([]);

  const table = useReactTable({
    data,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  const headerGroups = useMemo(() => table.getHeaderGroups(), [table]);
  const rows = table.getRowModel().rows;

  if (isLoading) {
    return <Spinner />;
  }

  if (!rows.length) {
    return <EmptyState title="Nenhum registro encontrado" description="Ajuste os filtros ou recarregue a tabela." />;
  }

  return (
    <div className="overflow-auto rounded-2xl border border-slate-200 bg-white shadow-card">
      <div className="max-h-[70vh] overflow-auto scrollbar-thin">
        <table className="w-max table-auto border-separate border-spacing-0 text-left text-sm">
          <thead className="sticky top-0 z-10 bg-slate-50">
            {headerGroups.map((headerGroup) => (
              <tr key={headerGroup.id}>
                {headerGroup.headers.map((header) => (
                  <th
                    key={header.id}
                    className="border-b border-slate-200 px-4 py-3 text-xs font-semibold uppercase tracking-wide text-slate-500"
                  >
                    {header.isPlaceholder ? null : (
                      <button
                        type="button"
                        onClick={header.column.getToggleSortingHandler()}
                        className="flex min-w-0 items-center gap-2"
                      >
                        <span className="min-w-0 truncate">
                          {flexRender(header.column.columnDef.header, header.getContext())}
                        </span>
                        {header.column.getIsSorted() === 'asc' && '▲'}
                        {header.column.getIsSorted() === 'desc' && '▼'}
                      </button>
                    )}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {rows.map((row, idx) => (
              <tr
                key={row.id}
                onClick={() => onRowClick?.(row.original)}
                className={clsx(
                  'cursor-pointer transition',
                  idx % 2 === 0 ? 'bg-white' : 'bg-slate-100',
                  'hover:bg-slate-100',
                )}
              >
                {row.getVisibleCells().map((cell) => {
                  const rendered = flexRender(cell.column.columnDef.cell, cell.getContext());
                  const rawValue = cell.getValue();
                  const title =
                    typeof rendered === 'string' || typeof rendered === 'number'
                      ? String(rendered)
                      : typeof rawValue === 'string' || typeof rawValue === 'number'
                        ? String(rawValue)
                        : undefined;

                  return (
                    <td key={cell.id} className="border-b border-slate-100 px-4 py-3 text-slate-700">
                      <div className="whitespace-nowrap" title={title}>
                        {rendered}
                      </div>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};
