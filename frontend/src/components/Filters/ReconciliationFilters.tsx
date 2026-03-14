import { ChangeEvent } from 'react';
import { Button } from '../common/Button';
import { Input } from '../common/Input';
import { Select } from '../common/Select';
import { Card } from '../common/Card';

export type ReconciliationFiltersValues = {
  dateFrom: string;
  dateTo: string;
  status: string;
  search: string;
  paymentType?: string;
  brand?: string;
};

interface FiltersProps {
  draftFilters: ReconciliationFiltersValues;
  onDraftChange: (values: ReconciliationFiltersValues) => void;
  onApply: () => void;
  onReset: () => void;
  isFetching?: boolean;
  showAdvanced?: boolean;
  dateError?: string;
  statusOptions?: string[];
  paymentOptions?: string[];
  brandOptions?: string[];
  brandDisabled?: boolean;
}

export const ReconciliationFilters = ({
  draftFilters,
  onDraftChange,
  onApply,
  onReset,
  isFetching,
  showAdvanced = true,
  dateError,
  statusOptions = [],
  paymentOptions = [],
  brandOptions = [],
  brandDisabled = false,
}: FiltersProps) => {
  const paymentOptionLabel = (option: string) => {
    if (option === 'CARD') {
      return 'CARTÃO';
    }
    return option;
  };

  const updateField =
    (field: keyof ReconciliationFiltersValues) =>
    (event: ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    onDraftChange({ ...draftFilters, [field]: event.target.value });
  };

  return (
    <Card className="mb-1 border border-slate-100 bg-white/95 backdrop-blur">
      <form
        onSubmit={(event) => {
          event.preventDefault();
          onApply();
        }}
        className="flex flex-wrap items-end gap-2 p-2"
      >
        <div className="w-[120px]">
          <label className="mb-0.5 block text-[10px] font-semibold uppercase leading-none text-slate-500">De</label>
          <Input
            type="date"
            className="h-9 px-2 text-sm"
            value={draftFilters.dateFrom}
            onChange={updateField('dateFrom')}
          />
        </div>
        <div className="w-[120px]">
          <label className="mb-0.5 block text-[10px] font-semibold uppercase leading-none text-slate-500">Ate</label>
          <Input
            type="date"
            className="h-9 px-2 text-sm"
            value={draftFilters.dateTo}
            onChange={updateField('dateTo')}
          />
        </div>
        {showAdvanced ? (
          <div className="w-[120px]">
            <label className="mb-0.5 block text-[10px] font-semibold uppercase leading-none text-slate-500">Status</label>
            <Select
              className="h-9 px-2 text-sm"
              value={draftFilters.status}
              onChange={updateField('status')}
            >
              <option value="">Todos</option>
              {statusOptions.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </Select>
          </div>
        ) : null}
        {showAdvanced ? (
          <div className="w-[120px]">
            <label className="mb-0.5 block text-[10px] font-semibold uppercase leading-none text-slate-500">Forma de Pagamento</label>
            <Select
              className="h-9 px-2 text-sm"
              value={draftFilters.paymentType ?? ''}
              onChange={updateField('paymentType')}
            >
              <option value="">Todos</option>
              {paymentOptions.map((option) => (
                <option key={option} value={option}>
                  {paymentOptionLabel(option)}
                </option>
              ))}
            </Select>
          </div>
        ) : null}
        {showAdvanced ? (
          <div className="w-[110px]">
            <label className="mb-0.5 block text-[10px] font-semibold uppercase leading-none text-slate-500">Tipo</label>
            <Select
              className="h-9 px-2 text-sm"
              value={draftFilters.brand ?? ''}
              onChange={updateField('brand')}
              disabled={brandDisabled}
            >
              <option value="">Todas</option>
              {brandOptions.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </Select>
          </div>
        ) : null}
        {showAdvanced ? (
          <div className="w-full min-w-0 flex-1">
            <label className="mb-0.5 block text-[10px] font-semibold uppercase leading-none text-slate-500">Busca</label>
            <Input
              placeholder="NSU, venda, autorizacao, texto livre"
              className="h-9 px-2 text-sm"
              value={draftFilters.search}
              onChange={updateField('search')}
            />
          </div>
        ) : null}
        <div className="flex flex-wrap justify-end gap-2">
          <Button type="submit" className="h-9 px-3 py-0 text-xs" disabled={isFetching}>
            Aplicar
          </Button>
          <Button
            type="button"
            variant="outline"
            className="h-9 px-3 py-0 text-xs"
            onClick={onReset}
            disabled={isFetching}
          >
            Limpar
          </Button>
        </div>
      </form>
      {dateError ? (
        <p className="px-3 pb-2 text-[11px] text-amber-600">{dateError}</p>
      ) : null}
    </Card>
  );
};
