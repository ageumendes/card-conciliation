export const formatCurrency = (value?: number | null) => {
  if (value === null || value === undefined) {
    return '-';
  }
  return new Intl.NumberFormat('pt-BR', {
    style: 'currency',
    currency: 'BRL',
  }).format(value);
};

export const formatDateTime = (value?: string | null) => {
  if (!value) {
    return '-';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat('pt-BR', {
    dateStyle: 'short',
    timeStyle: 'short',
  }).format(date);
};

export const formatDateTimeCompact = (value?: string | null) => {
  if (!value) {
    return '';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  const pad = (num: number) => String(num).padStart(2, '0');
  return `${pad(date.getDate())}/${pad(date.getMonth() + 1)}/${date.getFullYear()} ${pad(
    date.getHours(),
  )}:${pad(date.getMinutes())}`;
};

export const maskDateInput = (value: string): string => {
  const raw = value.replace(/[^\d/]/g, '');
  const pad = (part: string) => (part.length === 1 ? part.padStart(2, '0') : part);
  let day = '';
  let month = '';
  let year = '';

  if (raw.includes('/')) {
    const [dayRaw = '', monthRaw = '', ...rest] = raw.split('/');
    day = dayRaw.replace(/\D/g, '').slice(0, 2);
    month = monthRaw.replace(/\D/g, '').slice(0, 2);
    year = rest.join('').replace(/\D/g, '').slice(0, 4);
    if (!year) {
      const monthDigits = monthRaw.replace(/\D/g, '');
      if (monthDigits.length > 2) {
        month = monthDigits.slice(0, 2);
        year = monthDigits.slice(2, 6);
      }
    }
  } else {
    const digits = raw.replace(/\D/g, '').slice(0, 8);
    day = digits.slice(0, 2);
    month = digits.slice(2, 4);
    year = digits.slice(4, 8);
  }

  if (year.length === 4) {
    day = pad(day);
    month = pad(month);
  }

  const parts = [day, month, year].filter((part) => part.length > 0);
  return parts.join('/');
};
