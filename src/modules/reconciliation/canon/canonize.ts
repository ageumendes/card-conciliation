type CanonMethod =
  | 'PIX'
  | 'DEBIT'
  | 'CREDIT'
  | 'VOUCHER'
  | 'CARD'
  | 'DEBITO'
  | 'CREDITO'
  | 'CREDITO_A_VISTA';
type CanonMethodGroup = 'PIX' | 'CARD';

export const canonTerminal = (raw?: string | null): {
  digits: string | null;
  ltrim0: string | null;
  last6: string | null;
  pad9: string | null;
} => {
  const digits = String(raw ?? '').replace(/\D/g, '');
  if (!digits) {
    return { digits: null, ltrim0: null, last6: null, pad9: null };
  }
  const ltrim0 = digits.replace(/^0+/, '') || null;
  const last6 = digits.length >= 6 ? digits.slice(-6) : null;
  const pad9 = digits.padStart(9, '0');
  return { digits, ltrim0, last6, pad9 };
};

const toUpperTrim = (value?: unknown): string => {
  return String(value ?? '').trim().toUpperCase();
};

const dateOnly = (value: Date | string | null | undefined): string | null => {
  if (!value) {
    return null;
  }
  if (value instanceof Date) {
    const y = value.getFullYear();
    const m = String(value.getMonth() + 1).padStart(2, '0');
    const d = String(value.getDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
  }
  const text = String(value).trim();
  if (!text) {
    return null;
  }
  if (/^\d{4}-\d{2}-\d{2}/.test(text)) {
    return text.slice(0, 10);
  }
  return null;
};

const pickVoucherBrand = (text: string): string | null => {
  const vouchers = ['VIDALINK', 'CONVCARD', 'SODEXO', 'TICKET', 'ALELO', 'VR'];
  for (const voucher of vouchers) {
    if (text.includes(voucher)) {
      return voucher;
    }
  }
  return null;
};

const pickCardBrand = (text: string): string | null => {
  const brands = ['VISA', 'MASTERCARD', 'MASTER', 'ELO', 'CABAL', 'AMEX', 'AMERICAN EXPRESS', 'HIPERCARD', 'HIPER', 'DINERS', 'DISCOVER'];
  for (const brand of brands) {
    if (text.includes(brand)) {
      return brand === 'MASTER' ? 'MASTERCARD' : brand;
    }
  }
  return null;
};

const groupMethod = (method: CanonMethod | null): CanonMethodGroup | null => {
  if (!method) {
    return null;
  }
  return method === 'PIX' ? 'PIX' : 'CARD';
};

export const canonizeInterdata = (row: {
  SALE_DATETIME?: Date | string | null;
  GROSS_AMOUNT?: number | null;
  AUTH_NSU?: string | null;
  INSTALLMENTS?: number | null;
  CARD_BRAND_RAW?: string | null;
}): {
  CANON_SALE_DATE: string | null;
  CANON_METHOD: CanonMethod | null;
  CANON_METHOD_GROUP: CanonMethodGroup | null;
  CANON_BRAND: string | null;
  CANON_TERMINAL_NO: string | null;
  CANON_GROSS_AMOUNT: number | null;
  CANON_INSTALLMENT_NO: number | null;
  CANON_INSTALLMENT_TOTAL: number | null;
} => {
  const rawBrand = toUpperTrim(row.CARD_BRAND_RAW);
  const rawBrandNorm = rawBrand.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  let method: CanonMethod = 'CARD';
  let brand: string | null = null;

  if (rawBrandNorm.includes('PIX') || rawBrandNorm.includes('QR') || rawBrandNorm.includes('QRCODE') || rawBrandNorm.includes('QR_CODE')) {
    method = 'PIX';
    brand = 'PIX';
  } else {
    const voucher = pickVoucherBrand(rawBrandNorm);
    if (voucher) {
      method = 'VOUCHER';
      brand = voucher;
    } else if (rawBrandNorm.includes('DEBIT') || rawBrandNorm.includes('DEBITO')) {
      method = 'DEBIT';
      brand = pickCardBrand(rawBrandNorm);
    } else if (rawBrandNorm.includes('CRED') || rawBrandNorm.includes('CREDITO')) {
      method = 'CREDIT';
      brand = pickCardBrand(rawBrandNorm);
    } else if (rawBrandNorm.includes('VISA')) {
      method = 'CARD';
      brand = 'VISA';
    } else {
      method = 'CARD';
      brand = pickCardBrand(rawBrandNorm);
    }
  }

  return {
    CANON_SALE_DATE: dateOnly(row.SALE_DATETIME),
    CANON_METHOD: method,
    CANON_METHOD_GROUP: groupMethod(method),
    CANON_BRAND: brand,
    CANON_TERMINAL_NO: canonTerminal(row.AUTH_NSU ?? null).digits,
    CANON_GROSS_AMOUNT: typeof row.GROSS_AMOUNT === 'number' ? row.GROSS_AMOUNT : null,
    CANON_INSTALLMENT_NO: 1,
    CANON_INSTALLMENT_TOTAL: row.INSTALLMENTS ?? 1,
  };
};

export const canonizeCielo = (row: {
  SALE_DATETIME?: Date | string | null;
  GROSS_AMOUNT?: number | null;
  FEE_AMOUNT?: number | null;
  NET_AMOUNT?: number | null;
  MACHINE_NUMBER?: string | null;
  E_LOGICAL_TERMINAL_NO?: string | null;
  AUTH_CODE?: string | null;
  NSU_DOC?: string | null;
  E_INSTALLMENT_TOTAL?: number | null;
  E_INSTALLMENT_NO?: number | null;
  PAYMENT_METHOD?: string | null;
  ENTRY_TYPE?: string | null;
  BRAND?: string | null;
}): {
  CANON_SALE_DATE: string | null;
  CANON_METHOD: CanonMethod | null;
  CANON_METHOD_GROUP: CanonMethodGroup | null;
  CANON_BRAND: string | null;
  CANON_TERMINAL_NO: string | null;
  CANON_AUTH_CODE: string | null;
  CANON_NSU: string | null;
  CANON_GROSS_AMOUNT: number | null;
  CANON_FEE_AMOUNT: number | null;
  CANON_NET_AMOUNT: number | null;
  CANON_PERC_TAXA: number | null;
  CANON_INSTALLMENT_NO: number | null;
  CANON_INSTALLMENT_TOTAL: number | null;
} => {
  const paymentRaw = toUpperTrim(row.PAYMENT_METHOD ?? row.ENTRY_TYPE);
  const paymentNorm = paymentRaw.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  const entryNorm = toUpperTrim(row.ENTRY_TYPE).normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  const brandNorm = toUpperTrim(row.BRAND).normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  let method: CanonMethod | null = null;
  if (paymentNorm.includes('PIX')) {
    method = 'PIX';
  } else if (paymentNorm.includes('CREDITO_A_VISTA') || paymentNorm.includes('CREDITO A VISTA')) {
    method = 'CREDITO_A_VISTA';
  } else if (paymentNorm.includes('DEBIT') || paymentNorm.includes('DEBITO')) {
    method = 'DEBITO';
  } else if (paymentNorm.includes('CRED') || paymentNorm.includes('CREDITO')) {
    method = 'CREDITO';
  } else if (paymentNorm.includes('VOUCHER')) {
    method = 'VOUCHER';
  }
  if (!method) {
    if (entryNorm.includes('PIX') || brandNorm.includes('PIX')) {
      method = 'PIX';
    } else if (entryNorm.includes('DEBIT') || entryNorm.includes('DEBITO')) {
      method = 'DEBITO';
    } else if (entryNorm.includes('CRED') || entryNorm.includes('CREDITO')) {
      method = 'CREDITO_A_VISTA';
    } else if (pickVoucherBrand(brandNorm)) {
      method = 'VOUCHER';
    } else if (brandNorm) {
      // For card brands (ex: DISCOVER, VISA, MASTERCARD) without decoded payment method,
      // keep deterministic CARD group and avoid null canonical method.
      method = 'CREDITO_A_VISTA';
    }
  }

  const gross = typeof row.GROSS_AMOUNT === 'number' ? row.GROSS_AMOUNT : null;
  const fee = typeof row.FEE_AMOUNT === 'number' ? row.FEE_AMOUNT : null;
  const net = typeof row.NET_AMOUNT === 'number' ? row.NET_AMOUNT : null;
  const percTaxa =
    gross !== null && gross !== 0 && fee !== null
      ? Number(((fee / gross) * 100).toFixed(4))
      : null;

  return {
    CANON_SALE_DATE: dateOnly(row.SALE_DATETIME),
    CANON_METHOD: method,
    CANON_METHOD_GROUP: groupMethod(method),
    CANON_BRAND: toUpperTrim(row.BRAND) || null,
    CANON_TERMINAL_NO: canonTerminal(row.MACHINE_NUMBER?.trim() || row.E_LOGICAL_TERMINAL_NO?.trim() || null)
      .digits,
    CANON_AUTH_CODE: row.AUTH_CODE?.trim() || null,
    CANON_NSU: row.NSU_DOC?.trim() || null,
    CANON_GROSS_AMOUNT: gross,
    CANON_FEE_AMOUNT: fee,
    CANON_NET_AMOUNT: net,
    CANON_PERC_TAXA: percTaxa,
    CANON_INSTALLMENT_NO: row.E_INSTALLMENT_NO ?? 1,
    CANON_INSTALLMENT_TOTAL: row.E_INSTALLMENT_TOTAL ?? 1,
  };
};

export const canonizeSipag = (row: {
  SALE_DATETIME?: Date | string | null;
  GROSS_AMOUNT?: number | null;
  FEE_AMOUNT?: number | null;
  NET_AMOUNT?: number | null;
  PAYMENT_METHOD?: string | null;
  CREDIT_DEBIT_IND?: string | null;
  PLAN_DESC?: string | null;
  CARD_TYPE?: string | null;
  BRAND?: string | null;
  TERMINAL_NO?: string | null;
  AUTH_NO?: string | null;
  TRANSACTION_NO?: string | null;
  SALE_ID?: string | null;
  INSTALLMENT_NO?: number | null;
  INSTALLMENT_TOTAL?: number | null;
}): {
  CANON_SALE_DATE: string | null;
  CANON_METHOD: CanonMethod | null;
  CANON_METHOD_GROUP: CanonMethodGroup | null;
  CANON_BRAND: string | null;
  CANON_TERMINAL_NO: string | null;
  CANON_AUTH_CODE: string | null;
  CANON_NSU: string | null;
  CANON_GROSS_AMOUNT: number | null;
  CANON_FEE_AMOUNT: number | null;
  CANON_NET_AMOUNT: number | null;
  CANON_PERC_TAXA: number | null;
  CANON_INSTALLMENT_NO: number | null;
  CANON_INSTALLMENT_TOTAL: number | null;
} => {
  const methodRaw = toUpperTrim(
    [
      row.PAYMENT_METHOD,
      row.CREDIT_DEBIT_IND,
      row.PLAN_DESC,
      row.CARD_TYPE,
      row.BRAND,
    ]
      .filter(Boolean)
      .join(' '),
  ).normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  let method: CanonMethod = 'CARD';
  if (methodRaw.includes('PIX')) {
    method = 'PIX';
  } else if (
    methodRaw.includes('DEBIT') ||
    methodRaw.includes('DEBITO') ||
    methodRaw === 'D'
  ) {
    method = 'DEBIT';
  } else if (
    methodRaw.includes('CRED') ||
    methodRaw.includes('CREDITO') ||
    methodRaw === 'C'
  ) {
    method = 'CREDIT';
  } else if (
    methodRaw.includes('VOUCHER') ||
    methodRaw.includes('ALELO') ||
    methodRaw.includes('SODEXO') ||
    methodRaw.includes('TICKET') ||
    methodRaw.includes('VR')
  ) {
    method = 'VOUCHER';
  }

  const rawBrand = toUpperTrim(row.BRAND);
  const sanitizedBrand = rawBrand
    .replace(/^\d+\s*-\s*/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  const normalizedBrandText = sanitizedBrand.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  const brandFromCard = pickCardBrand(normalizedBrandText);
  const brandFromVoucher = pickVoucherBrand(normalizedBrandText);
  const brand =
    brandFromCard ??
    brandFromVoucher ??
    (sanitizedBrand === 'MASTER' ? 'MASTERCARD' : sanitizedBrand || null);
  const gross = typeof row.GROSS_AMOUNT === 'number' ? row.GROSS_AMOUNT : null;
  const fee = typeof row.FEE_AMOUNT === 'number' ? row.FEE_AMOUNT : null;
  const net = typeof row.NET_AMOUNT === 'number' ? row.NET_AMOUNT : null;
  const percTaxa =
    gross !== null && gross !== 0 && fee !== null
      ? Number(((fee / gross) * 100).toFixed(4))
      : null;

  return {
    CANON_SALE_DATE: dateOnly(row.SALE_DATETIME),
    CANON_METHOD: method,
    CANON_METHOD_GROUP: groupMethod(method),
    CANON_BRAND: method === 'PIX' ? 'PIX' : brand,
    CANON_TERMINAL_NO: canonTerminal(row.TERMINAL_NO ?? null).digits,
    CANON_AUTH_CODE: row.AUTH_NO?.trim() || null,
    CANON_NSU: row.TRANSACTION_NO?.trim() || row.SALE_ID?.trim() || null,
    CANON_GROSS_AMOUNT: gross,
    CANON_FEE_AMOUNT: fee,
    CANON_NET_AMOUNT: net,
    CANON_PERC_TAXA: percTaxa,
    CANON_INSTALLMENT_NO: row.INSTALLMENT_NO ?? 1,
    CANON_INSTALLMENT_TOTAL: row.INSTALLMENT_TOTAL ?? 1,
  };
};

export const canonizeSicredi = (row: {
  SALE_DATETIME?: Date | string | null;
  GROSS_AMOUNT?: number | null;
  MDR_AMOUNT?: number | null;
  NET_AMOUNT?: number | null;
  SECTION_KIND?: string | null;
  PRODUCT?: string | null;
  CARD_TYPE?: string | null;
  CARD_SCHEME_CODE?: string | null;
  CARD_SCHEME_DESC?: string | null;
  BRAND?: string | null;
  TERMINAL_NO?: string | null;
  AUTH_CODE?: string | null;
  SALE_RECEIPT?: string | null;
  PAYMENT_CODE?: string | null;
  CARD_REF_CODE?: string | null;
}): {
  CANON_SALE_DATE: string | null;
  CANON_METHOD: CanonMethod | null;
  CANON_METHOD_GROUP: CanonMethodGroup | null;
  CANON_BRAND: string | null;
  CANON_TERMINAL_NO: string | null;
  CANON_AUTH_CODE: string | null;
  CANON_NSU: string | null;
  CANON_GROSS_AMOUNT: number | null;
  CANON_FEE_AMOUNT: number | null;
  CANON_NET_AMOUNT: number | null;
  CANON_PERC_TAXA: number | null;
  CANON_INSTALLMENT_NO: number | null;
  CANON_INSTALLMENT_TOTAL: number | null;
} => {
  const methodRaw = toUpperTrim(
    [row.SECTION_KIND, row.PRODUCT, row.CARD_TYPE, row.CARD_SCHEME_CODE, row.CARD_SCHEME_DESC, row.BRAND]
      .filter(Boolean)
      .join(' '),
  )
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');
  const sectionKind = toUpperTrim(row.SECTION_KIND);
  const cardSchemeCode = toUpperTrim(row.CARD_SCHEME_CODE);
  const cardSchemeDesc = toUpperTrim(row.CARD_SCHEME_DESC)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');

  let method: CanonMethod = 'CARD';
  if (methodRaw.includes('PIX')) {
    method = 'PIX';
  } else if (
    sectionKind === 'DEBIT' ||
    cardSchemeCode.endsWith('D') ||
    cardSchemeDesc.includes('DEBIT')
  ) {
    method = 'DEBITO';
  } else if (
    sectionKind === 'CREDIT' ||
    cardSchemeCode.endsWith('C') ||
    cardSchemeDesc.includes('CREDIT')
  ) {
    method = 'CREDITO';
  } else if (methodRaw.includes('VOUCHER') || methodRaw.includes('ALELO') || methodRaw.includes('SODEXO') || methodRaw.includes('TICKET') || methodRaw.includes('VR')) {
    method = 'VOUCHER';
  } else if (methodRaw.includes('DEBIT') || methodRaw.includes('DEBITO')) {
    method = 'DEBITO';
  } else if (methodRaw.includes('CRED') || methodRaw.includes('CREDITO')) {
    method = 'CREDITO';
  }

  const rawBrand = toUpperTrim(row.BRAND);
  const sanitizedBrand = rawBrand
    .replace(/^\d+\s*-\s*/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  const normalizedBrandText = sanitizedBrand.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  const normalizedSchemeText = cardSchemeDesc || normalizedBrandText;
  const brandFromCard = pickCardBrand(normalizedBrandText);
  const brandFromVoucher = pickVoucherBrand(normalizedBrandText);
  const brand =
    method === 'PIX'
      ? 'PIX'
      : brandFromCard ??
        pickCardBrand(normalizedSchemeText) ??
        brandFromVoucher ??
        (sanitizedBrand === 'MASTER' ? 'MASTERCARD' : sanitizedBrand || null);

  const gross = typeof row.GROSS_AMOUNT === 'number' ? row.GROSS_AMOUNT : null;
  const fee = typeof row.MDR_AMOUNT === 'number' ? row.MDR_AMOUNT : null;
  const net = typeof row.NET_AMOUNT === 'number' ? row.NET_AMOUNT : null;
  const percTaxa =
    gross !== null && gross !== 0 && fee !== null
      ? Number(((fee / gross) * 100).toFixed(4))
      : null;

  return {
    CANON_SALE_DATE: dateOnly(row.SALE_DATETIME),
    CANON_METHOD: method,
    CANON_METHOD_GROUP: groupMethod(method),
    CANON_BRAND: brand,
    CANON_TERMINAL_NO: canonTerminal(row.TERMINAL_NO ?? null).digits,
    CANON_AUTH_CODE: row.AUTH_CODE?.trim() || null,
    CANON_NSU: row.SALE_RECEIPT?.trim() || row.PAYMENT_CODE?.trim() || row.CARD_REF_CODE?.trim() || null,
    CANON_GROSS_AMOUNT: gross,
    CANON_FEE_AMOUNT: fee,
    CANON_NET_AMOUNT: net,
    CANON_PERC_TAXA: percTaxa,
    CANON_INSTALLMENT_NO: 1,
    CANON_INSTALLMENT_TOTAL: 1,
  };
};
