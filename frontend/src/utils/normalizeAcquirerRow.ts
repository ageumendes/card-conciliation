const normalizeStatusText = (value: unknown) => {
  const raw = String(value ?? '').trim();
  if (!raw) {
    return null;
  }
  const normalized = raw
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();
  if (
    normalized.startsWith('undone') ||
    normalized === 'unauthorized - 90' ||
    normalized === 'cancelado'
  ) {
    return 'Cancelado';
  }
  return raw;
};

export function normalizeAcquirerRow(input: any) {
  const raw = input?.raw ?? {};

  return {
    id: Number(input?.rowId ?? raw.ID),

    datetimeISO: raw.SALE_DATETIME ?? '',
    datetimeText: input?.datetimeText ?? '',

    operatorKey: input?.operatorKey ?? '',
    operatorText: input?.operatorText ?? '',

    gross: raw.GROSS_AMOUNT ?? null,
    fee: raw.FEE_AMOUNT ?? null,
    net: raw.NET_AMOUNT ?? null,

    status: normalizeStatusText(raw.STATUS ?? input?.statusText ?? null),
    entryType: raw.ENTRY_TYPE ?? null,
    reason: raw.REASON ?? input?.reasonText ?? null,

    ids: {
      establishmentNo: raw.ESTABLISHMENT_NO ?? undefined,
      authCode: raw.AUTH_CODE ?? undefined,
      nsuDoc: raw.NSU_DOC ?? undefined,
      tid: raw.TID ?? undefined,
      machineNumber: raw.MACHINE_NUMBER ?? undefined,
      rowHash: raw.ROW_HASH ?? undefined,
    },

    details: {
      paymentMethod: raw.PAYMENT_METHOD ?? null,
      brand: raw.BRAND ?? null,
      settlementDate: raw.SETTLEMENT_DATE ?? null,
      entryDate: raw.ENTRY_DATE ?? null,
      createdAt: raw.CREATED_AT ?? null,
    },
  };
}
