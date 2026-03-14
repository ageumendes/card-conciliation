import { Logger } from '@nestjs/common';
import { createHash } from 'crypto';
import {
  EdiAdjustment,
  EdiAdjustmentItem,
  EdiFileType,
  EdiTransaction,
  ParsedCielo04File,
  ParsedCielo16File,
  ParsedEdiFile,
  Cielo04AdjustmentBlock,
} from './cielo-sftp-edi.types';

const logger = new Logger('CieloEdiParser');
const isDebugEnabled = () => process.env.DEBUG === 'true';

const detectDelimiter = (line: string): string | null => {
  const candidates = ['|', ';', ',', '\t'];
  for (const candidate of candidates) {
    if (line.includes(candidate)) {
      return candidate === '\t' ? '\t' : candidate;
    }
  }
  return null;
};

const sliceFixed = (line: string, start: number, end: number): string => {
  return line.slice(start - 1, end).trim();
};

const sliceRaw = (line: string, start: number, end: number): string => {
  return line.slice(start - 1, end);
};

const sliceText = (line: string, start: number, end: number): string => {
  return sliceRaw(line, start, end).trim();
};

const parseMoneySigned14 = (raw: string): number | null => {
  const value = raw.trim();
  if (!value) {
    return null;
  }
  const sign = value.startsWith('-') ? -1 : 1;
  const digits = value.replace(/^[+-]/, '').replace(/\D/g, '');
  if (!digits) {
    return null;
  }
  const amount = sign * Number(digits) / 100;
  return Math.round(amount * 100) / 100;
};

const parseDateTime = (dateValue: string, timeValue?: string): Date | null => {
  const dateRaw = dateValue.trim();
  if (!dateRaw) {
    return null;
  }
  let datePart = dateRaw;
  if (/^\d{8}$/.test(dateRaw)) {
    datePart = `${dateRaw.slice(0, 4)}-${dateRaw.slice(4, 6)}-${dateRaw.slice(6, 8)}`;
  }
  const timeRaw = (timeValue ?? '').trim();
  const paddedTime = (timeRaw || '000000').padEnd(6, '0');
  const timePart = `${paddedTime.slice(0, 2)}:${paddedTime.slice(2, 4)}:${paddedTime.slice(4, 6)}`;
  const timestamp = `${datePart}T${timePart}`;
  const parsed = new Date(timestamp);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed;
};

const formatTimeFromHHMMSS = (raw: string | null): string | null => {
  if (!raw || raw.length !== 6 || !hasDigitsOnly(raw)) {
    return null;
  }
  const hh = raw.slice(0, 2);
  const mm = raw.slice(2, 4);
  const ss = raw.slice(4, 6);
  return `${hh}:${mm}:${ss}`;
};

const parseSignedAmount = (signChar: string, digits13: string): number | null => {
  const digits = digits13.replace(/\D/g, '');
  if (!digits) {
    return null;
  }
  const sign = signChar === '-' ? -1 : 1;
  const amount = sign * Number(digits) / 100;
  return Math.round(amount * 100) / 100;
};

const parseSignedMoney = (
  rawSign: string,
  rawDigits: string,
  context?: { field: string; range: string; rawLine?: string; lineNo?: number },
): { sign: '+' | '-'; amount: number | null; digits: string } => {
  const signChar = rawSign.trim();
  if (signChar && signChar !== '+' && signChar !== '-') {
    const lineInfo = context?.lineNo ? ` line=${context.lineNo}` : '';
    const rawInfo = context?.rawLine ? ` raw=${context.rawLine.trimEnd()}` : '';
    throw new Error(
      `CIELO04D invalid sign for ${context?.field ?? 'amount'}: sign='${signChar}' range=${context?.range ?? ''}${lineInfo}${rawInfo}`,
    );
  }
  const sign: '+' | '-' = signChar === '-' ? '-' : '+';
  const digits = rawDigits.trim();
  if (!digits) {
    return { sign, amount: null, digits };
  }
  if (!hasDigitsOnly(digits)) {
    const lineInfo = context?.lineNo ? ` line=${context.lineNo}` : '';
    const rawInfo = context?.rawLine ? ` raw=${context.rawLine.trimEnd()}` : '';
    throw new Error(
      `CIELO04D invalid digits for ${context?.field ?? 'amount'}: digits='${digits}' range=${context?.range ?? ''}${lineInfo}${rawInfo}`,
    );
  }
  const amount = (parseInt(digits, 10) / 100) * (sign === '-' ? -1 : 1);
  return { sign, amount: Math.round(amount * 100) / 100, digits };
};

const hasDigitsOnly = (value: string): boolean => {
  if (!value) {
    return false;
  }
  for (const char of value) {
    if (char < '0' || char > '9') {
      return false;
    }
  }
  return true;
};

const parseHeaderDate = (line: string): string | null => {
  const raw = sliceRaw(line, 12, 19).trim();
  if (raw.length !== 8 || !hasDigitsOnly(raw)) {
    return null;
  }
  const year = raw.slice(0, 4);
  const month = raw.slice(4, 6);
  const day = raw.slice(6, 8);
  const monthNum = Number(month);
  const dayNum = Number(day);
  if (monthNum < 1 || monthNum > 12 || dayNum < 1 || dayNum > 31) {
    return null;
  }
  return `${year}-${month}-${day}`;
};

const parseDateDDMMAAAA = (raw: string): string | null => {
  const trimmed = raw.trim();
  if (trimmed.length !== 8 || !hasDigitsOnly(trimmed)) {
    return null;
  }
  const day = trimmed.slice(0, 2);
  const month = trimmed.slice(2, 4);
  const year = trimmed.slice(4, 8);
  const monthNum = Number(month);
  const dayNum = Number(day);
  if (monthNum < 1 || monthNum > 12 || dayNum < 1 || dayNum > 31) {
    return null;
  }
  return `${year}-${month}-${day}`;
};

const parseRate = (raw: string): number | null => {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  if (!hasDigitsOnly(trimmed)) {
    return null;
  }
  return Number(trimmed) / 10000;
};

const parseSignedAmountField = (
  line: string,
  signPos: number,
  start: number,
  end: number,
): { sign: string | null; amount: number | null } => {
  const sign = sliceRaw(line, signPos, signPos).trim() || null;
  const digits = sliceRaw(line, start, end).trim();
  if (!digits || !hasDigitsOnly(digits)) {
    return { sign, amount: null };
  }
  const multiplier = sign === '-' ? -1 : 1;
  const amount = (Number(digits) / 100) * multiplier;
  return { sign, amount: Math.round(amount * 100) / 100 };
};

const parseTimeHHMMSS = (raw: string): string | null => {
  const trimmed = raw.trim();
  if (trimmed.length !== 6 || !hasDigitsOnly(trimmed)) {
    return null;
  }
  const hh = Number(trimmed.slice(0, 2));
  const mm = Number(trimmed.slice(2, 4));
  const ss = Number(trimmed.slice(4, 6));
  if (hh < 0 || hh > 23 || mm < 0 || mm > 59 || ss < 0 || ss > 59) {
    return null;
  }
  return trimmed;
};

const parseDateYYYYMMDD = (raw: string): string | null => {
  const trimmed = raw.trim();
  if (trimmed.length !== 8 || !hasDigitsOnly(trimmed)) {
    return null;
  }
  const year = trimmed.slice(0, 4);
  const month = trimmed.slice(4, 6);
  const day = trimmed.slice(6, 8);
  const monthNum = Number(month);
  const dayNum = Number(day);
  if (monthNum < 1 || monthNum > 12 || dayNum < 1 || dayNum > 31) {
    return null;
  }
  return `${year}-${month}-${day}`;
};

const parseDateAAMMDD = (raw: string): string | null => {
  const trimmed = raw.trim();
  if (trimmed.length !== 6 || !hasDigitsOnly(trimmed)) {
    return null;
  }
  const yearShort = Number(trimmed.slice(0, 2));
  const month = trimmed.slice(2, 4);
  const day = trimmed.slice(4, 6);
  const monthNum = Number(month);
  const dayNum = Number(day);
  if (monthNum < 1 || monthNum > 12 || dayNum < 1 || dayNum > 31) {
    return null;
  }
  const year = 2000 + yearShort;
  return `${year}-${month}-${day}`;
};

const parseSignedAmountLine = (
  line: string,
  signPos: number,
  start: number,
  end: number,
): number | null => {
  const sign = sliceRaw(line, signPos, signPos).trim();
  const digits = sliceRaw(line, start, end).trim();
  if (!digits || !hasDigitsOnly(digits)) {
    return null;
  }
  const multiplier = sign === '-' ? -1 : 1;
  const amount = (Number(digits) / 100) * multiplier;
  return Math.round(amount * 100) / 100;
};

const parseCielo16Header = (line: string) => {
  if (!line.startsWith('0')) {
    return { headerDate: null, establishmentNo: null };
  }
  const establishmentNo = sliceText(line, 2, 11) || null;
  const headerDate = parseDateYYYYMMDD(sliceRaw(line, 12, 19));
  return { headerDate, establishmentNo };
};

const parseCielo16Detail = (
  line: string,
  headerDate: string | null,
  headerEstablishment: string | null,
  verbose?: boolean,
): EdiTransaction | null => {
  const debugEnabled = Boolean(verbose || isDebugEnabled());
  if (!line.startsWith('8')) {
    return null;
  }
  if (line.length < 400) {
    if (debugEnabled) {
      logger.warn(
        { linePreview: line.slice(0, 120), length: line.length },
        'CIELO16D line too short',
      );
    }
    return null;
  }
  const typeCode = sliceText(line, 12, 13);
  const dateTransacao = parseDateAAMMDD(sliceRaw(line, 14, 19));
  const timeTransacao = parseTimeHHMMSS(sliceRaw(line, 20, 25)) ?? '000000';
  const pixId = sliceRaw(line, 26, 61).trimEnd() || null;
  const nsu6 = sliceRaw(line, 62, 67).trim();
  const dataPagamento = parseDateAAMMDD(sliceRaw(line, 68, 73));
  const grossAmount = parseSignedAmountLine(line, 74, 75, 87);
  const feeAmount = parseSignedAmountLine(line, 88, 89, 101);
  const netAmount = parseSignedAmountLine(line, 102, 103, 115);
  const entryDate = parseDateAAMMDD(sliceRaw(line, 145, 150));
  const transferenciaAutomatica = sliceText(line, 222, 222).toUpperCase();
  const statusConta = sliceText(line, 223, 224);
  const dataContaCielo = parseDateAAMMDD(sliceRaw(line, 225, 230));
  const nsu8 = sliceRaw(line, 231, 238).trim();
  const txId = sliceRaw(line, 240, 275).trimEnd();
  const pixPaymentId = sliceRaw(line, 312, 347).trimEnd();

  const hasNonZero = (value: string) => /[1-9]/.test(value);
  const nsuDoc = hasNonZero(nsu8) ? nsu8 : nsu6 || null;
  const settlementDate =
    transferenciaAutomatica === 'S' &&
    statusConta === '05' &&
    dataContaCielo
      ? dataContaCielo
      : dataPagamento;

  const saleDatetime = dateTransacao
    ? `${dateTransacao} ${timeTransacao.slice(0, 2)}:${timeTransacao.slice(2, 4)}:${timeTransacao.slice(4, 6)}`
    : null;

  // STATUS rules for PIX: if not tipoTransacao '01' => AJUSTE,
  // if statusConta in 02/03/04 => PENDENTE,
  // else if tipoTransacao '01' => APROVADA.
  let status: string | null = null;
  if (typeCode && typeCode !== '01') {
    status = 'AJUSTE';
  } else if (statusConta === '02' || statusConta === '03' || statusConta === '04') {
    status = 'PENDENTE';
  } else if (typeCode === '01') {
    status = 'APROVADA';
  }

  return {
    saleDatetime,
    establishmentNo: headerEstablishment,
    paymentMethod: 'PIX',
    brand: 'PIX',
    grossAmount,
    feeAmount,
    netAmount,
    status,
    entryType: 'PIX',
    reason: null,
    entryDate,
    settlementDate,
    authCode: null,
    nsuDoc,
    saleCode: null,
    tid: null,
    machineNumber: null,
    rowHash: buildRowHash('CIELO16', line),
    pixId,
    txId: txId || (pixPaymentId || null),
    pixPaymentId: pixPaymentId || null,
    eRecordType: '8',
    eSubmitEstablishment: headerEstablishment,
    ePaymentMethodCode: null,
    eTimeHhmmss: timeTransacao,
    eFileHeaderDate: headerDate,
    eRawLine: line.trimEnd(),
    eEntryDate: entryDate,
  };
};

const buildSaleDatetime = (
  authDate: string | null,
  captureDate: string | null,
  entryDate: string | null,
  headerDate: string | null,
  timeRaw: string | null,
): string | null => {
  const saleDate = authDate || captureDate || entryDate || headerDate;
  if (!saleDate) {
    return null;
  }
  const timeValue = timeRaw && timeRaw !== '000000' ? timeRaw : '000000';
  const hh = timeValue.slice(0, 2);
  const mi = timeValue.slice(2, 4);
  const ss = timeValue.slice(4, 6);
  return `${saleDate} ${hh}:${mi}:${ss}`;
};

const mapBrand = (code: string): string | null => {
  const normalized = code.trim();
  if (!normalized) {
    return null;
  }
  const brandMap: Record<string, string> = {
    '001': 'VISA',
    '002': 'MASTERCARD',
    '003': 'ELO',
    '004': 'AMERICAN EXPRESS',
    '005': 'HIPERCARD',
    '006': 'DINERS CLUB',
    '007': 'DISCOVER',
    '008': 'JCB',
    '009': 'AURA',
    '010': 'CABAL',
    '011': 'UNIONPAY',
    '012': 'BANESCARD',
    '013': 'SOROCRED',
    '014': 'VEROCHEQUE',
    '015': 'ALELO',
    '016': 'SODEXO',
    '017': 'VR',
    '018': 'TICKET',
    '019': 'BEN',
    '020': 'GOOD CARD',
    '021': 'POLICARD',
    '022': 'VALECARD',
    '023': 'UP BRASIL',
    '024': 'GREENCARD',
    '025': 'COOPERCARD',
    '026': 'TRICARD',
    '027': 'MAIS',
    '028': 'NUTRICASH',
    '029': 'FLEX',
    '030': 'BANRICARD',
    '999': 'OUTRAS',
  };
  return brandMap[normalized] ?? 'OUTRAS';
};

const mapModality = (code: string): string | null => {
  switch (code.trim()) {
    case '001':
      return 'DÉBITO';
    case '002':
      return 'CRÉDITO';
    case '004':
      return 'VOUCHER';
    default:
      return null;
  }
};

const mapStatus = (flag: string): string | null => {
  const normalized = flag.trim().toUpperCase();
  if (normalized === 'N') {
    return 'Aprovada';
  }
  if (normalized === 'S') {
    return 'Rejeitada';
  }
  return null;
};

const buildRowHash = (type: EdiFileType, line: string): string => {
  return createHash('sha256')
    .update(`${type}:${line.trimEnd()}`)
    .digest('hex');
};

const buildCielo04RowHash = (recordType: 'D' | 'E', line: string): string => {
  return createHash('sha256')
    .update(`CIELO04D|${recordType}|${line.trimEnd()}`)
    .digest('hex');
};

const parseEdiTransactionLine = (
  type: EdiFileType,
  line: string,
  headerDate: string | null,
  filename?: string,
  verbose?: boolean,
): EdiTransaction => {
  const parseIntField = (raw: string): number | null => {
    const trimmed = raw.trim();
    if (!trimmed || !hasDigitsOnly(trimmed)) {
      return null;
    }
    return Number(trimmed);
  };

  const recordType = sliceText(line, 1, 1);
  const establishmentNo = sliceText(line, 2, 11);
  const liquidationBrand = sliceText(line, 12, 14);
  const liquidationType = sliceText(line, 15, 17);
  const installmentNo = parseIntField(sliceText(line, 18, 19));
  const installmentTotal = parseIntField(sliceText(line, 20, 21));
  const authCode = sliceText(line, 22, 27);
  const entryType = sliceText(line, 28, 29);
  const chaveUr = sliceText(line, 30, 129);
  const negotiationCode = sliceText(line, 130, 151);
  const adjustmentCode = sliceText(line, 152, 155);
  const paymentMethodCode = sliceText(line, 156, 158);
  const indicatorPromo = sliceText(line, 159, 159);
  const indicatorDcc = sliceText(line, 160, 160);
  const indicatorMinCommission = sliceText(line, 161, 161);
  const indicatorRaTc = sliceText(line, 162, 162);
  const indicatorZeroFee = sliceText(line, 163, 163);
  const indicatorRejectedRaw = sliceText(line, 164, 164).toUpperCase();
  const indicatorLateSale = sliceText(line, 165, 165);
  const cardBin = sliceText(line, 166, 171);
  const cardLast4 = sliceText(line, 172, 175);
  const nsuDoc = sliceText(line, 176, 181);
  const invoiceNo = sliceText(line, 182, 191);
  const tid = sliceText(line, 192, 211);
  const orderReference = sliceText(line, 212, 231);
  const mdrRate = parseRate(sliceRaw(line, 232, 236));
  const raRate = parseRate(sliceRaw(line, 237, 241));
  const saleRate = parseRate(sliceRaw(line, 242, 246));
  const totalAmount = parseSignedAmountField(line, 247, 248, 260);
  const grossAmount = parseSignedAmountField(line, 261, 262, 274);
  const netAmount = parseSignedAmountField(line, 275, 276, 288);
  const commissionAmount = parseSignedAmountField(line, 289, 290, 302);
  const minCommissionAmount = parseSignedAmountField(line, 303, 304, 316);
  const entryAmount = parseSignedAmountField(line, 317, 318, 330);
  const mdrFeeAmount = parseSignedAmountField(line, 331, 332, 344);
  const fastReceiveAmount = parseSignedAmountField(line, 345, 346, 358);
  const cashoutAmount = parseSignedAmountField(line, 359, 360, 372);
  const shipmentFeeAmount = parseSignedAmountField(line, 373, 374, 386);
  const pendingAmount = parseSignedAmountField(line, 387, 388, 400);
  const debtTotalAmount = parseSignedAmountField(line, 401, 402, 414);
  const chargedAmount = parseSignedAmountField(line, 415, 416, 428);
  const adminFeeAmount = parseSignedAmountField(line, 429, 430, 442);
  const promoAmount = parseSignedAmountField(line, 443, 444, 456);
  const dccAmount = parseSignedAmountField(line, 457, 458, 470);
  const timeHhmmssRaw = parseTimeHHMMSS(sliceRaw(line, 471, 476));
  const cardGroup = sliceText(line, 477, 478);
  const receiverDocument = sliceText(line, 479, 492);
  const authBrand = sliceText(line, 493, 495);
  const saleUniqueCode = sliceText(line, 496, 510);
  const saleOriginalCode = sliceText(line, 511, 525);
  const negotiationEffectId = sliceText(line, 526, 540);
  const salesChannel = sliceText(line, 541, 543);
  const terminalLogicNo = sliceText(line, 544, 551);
  const originalEntryType = sliceText(line, 552, 553);
  const transactionType = sliceText(line, 554, 556);
  const cieloUsage1 = sliceText(line, 557, 560);
  const pricingModelCode = sliceText(line, 561, 565);
  const authDate = parseDateDDMMAAAA(sliceRaw(line, 566, 573));
  const captureDate = parseDateDDMMAAAA(sliceRaw(line, 574, 581));
  const entryDate = parseDateDDMMAAAA(sliceRaw(line, 582, 589));
  const originalEntryDate = parseDateDDMMAAAA(sliceRaw(line, 590, 597));
  const batchNumber = sliceText(line, 598, 604);
  const processedTransactionNo = sliceText(line, 605, 626);
  const rejectionReasonRaw = sliceText(line, 627, 629);
  const settlementBlock = sliceText(line, 630, 651);
  const indicatorCustomerInstallment = sliceText(line, 652, 652);
  const bank = sliceText(line, 653, 656);
  const branch = sliceText(line, 657, 661);
  const account = sliceText(line, 662, 681);
  const accountDigit = sliceText(line, 682, 682);
  const arn = sliceText(line, 683, 705);
  const indicatorReceivableNegotiation = sliceText(line, 706, 706);
  const captureType = sliceText(line, 707, 708);
  const negotiatorDocument = sliceText(line, 709, 722);
  const cieloUsage2 = sliceText(line, 723, 760);
  const rawLine = line.trimEnd();
  const saleDatetime = buildSaleDatetime(
    authDate,
    captureDate,
    entryDate,
    headerDate,
    timeHhmmssRaw,
  );
  if (!saleDatetime && (verbose || isDebugEnabled())) {
    logger.debug(
      {
        filename,
        headerDate,
        saleDateRaw: authDate ?? captureDate ?? entryDate ?? headerDate,
        saleTimeRaw: timeHhmmssRaw ?? '',
        linePreview: line.slice(0, 120),
        reason: headerDate ? 'invalid-time-or-date' : 'missing-header-date',
      },
      'Cielo EDI datetime fallback',
    );
  }

  const brand = mapBrand(liquidationBrand) ?? mapBrand(authBrand);
  const paymentMethod = mapModality(paymentMethodCode);
  const rejectionReason = indicatorRejectedRaw === 'S' ? rejectionReasonRaw : null;

  return {
    saleDatetime,
    establishmentNo: establishmentNo || null,
    paymentMethod,
    brand,
    grossAmount: grossAmount.amount,
    feeAmount: commissionAmount.amount,
    netAmount: netAmount.amount,
    status: null,
    entryType: entryType || null,
    reason: rejectionReason || null,
    entryDate: null,
    settlementDate: null,
    authCode: authCode || null,
    nsuDoc: nsuDoc || null,
    saleCode: saleUniqueCode || null,
    tid: tid || null,
    machineNumber: terminalLogicNo || null,
    rowHash: buildRowHash(type, line),
    eRecordType: recordType || null,
    eSubmitEstablishment: establishmentNo || null,
    eLiquidationBrand: liquidationBrand || null,
    eLiquidationType: liquidationType || null,
    eInstallmentNo: installmentNo,
    eInstallmentTotal: installmentTotal,
    eAuthCode: authCode || null,
    eEntryType: entryType || null,
    eChaveUr: chaveUr || null,
    eNegotiationCode: negotiationCode || null,
    eAdjustmentCode: adjustmentCode || null,
    ePaymentMethodCode: paymentMethodCode || null,
    eIndicatorPromo: indicatorPromo || null,
    eIndicatorDcc: indicatorDcc || null,
    eIndicatorMinCommission: indicatorMinCommission || null,
    eIndicatorRaTc: indicatorRaTc || null,
    eIndicatorZeroFee: indicatorZeroFee || null,
    eIndicatorRejected: indicatorRejectedRaw || null,
    eIndicatorLateSale: indicatorLateSale || null,
    eCardBin: cardBin || null,
    eCardLast4: cardLast4 || null,
    eNsuDoc: nsuDoc || null,
    eInvoiceNo: invoiceNo || null,
    eTid: tid || null,
    eOrderReference: orderReference || null,
    eMdrRate: mdrRate,
    eRaRate: raRate,
    eSaleRate: saleRate,
    eTotalAmountSign: totalAmount.sign,
    eTotalAmount: totalAmount.amount,
    eGrossAmountSign: grossAmount.sign,
    eGrossAmount: grossAmount.amount,
    eNetAmountSign: netAmount.sign,
    eNetAmount: netAmount.amount,
    eCommissionSign: commissionAmount.sign,
    eCommissionAmount: commissionAmount.amount,
    eMinCommissionSign: minCommissionAmount.sign,
    eMinCommissionAmount: minCommissionAmount.amount,
    eEntrySign: entryAmount.sign,
    eEntryAmount: entryAmount.amount,
    eMdrFeeSign: mdrFeeAmount.sign,
    eMdrFeeAmount: mdrFeeAmount.amount,
    eFastReceiveSign: fastReceiveAmount.sign,
    eFastReceiveAmount: fastReceiveAmount.amount,
    eCashoutSign: cashoutAmount.sign,
    eCashoutAmount: cashoutAmount.amount,
    eShipmentFeeSign: shipmentFeeAmount.sign,
    eShipmentFeeAmount: shipmentFeeAmount.amount,
    ePendingSign: pendingAmount.sign,
    ePendingAmount: pendingAmount.amount,
    eDebtTotalSign: debtTotalAmount.sign,
    eDebtTotalAmount: debtTotalAmount.amount,
    eChargedSign: chargedAmount.sign,
    eChargedAmount: chargedAmount.amount,
    eAdminFeeSign: adminFeeAmount.sign,
    eAdminFeeAmount: adminFeeAmount.amount,
    ePromoSign: promoAmount.sign,
    ePromoAmount: promoAmount.amount,
    eDccSign: dccAmount.sign,
    eDccAmount: dccAmount.amount,
    eTimeHhmmss: timeHhmmssRaw ?? '000000',
    eCardGroup: cardGroup || null,
    eReceiverDocument: receiverDocument || null,
    eAuthBrand: authBrand || null,
    eSaleUniqueCode: saleUniqueCode || null,
    eSaleOriginalCode: saleOriginalCode || null,
    eNegotiationEffectId: negotiationEffectId || null,
    eSalesChannel: salesChannel || null,
    eTerminalLogicNo: terminalLogicNo || null,
    eOriginalEntryType: originalEntryType || null,
    eTransactionType: transactionType || null,
    eCieloUsage1: cieloUsage1 || null,
    ePricingModelCode: pricingModelCode || null,
    eAuthDate: authDate,
    eCaptureDate: captureDate,
    eEntryDate: entryDate,
    eOriginalEntryDate: originalEntryDate,
    eBatchNumber: batchNumber || null,
    eProcessedTransactionNo: processedTransactionNo || null,
    eRejectionReason: rejectionReason,
    eSettlementBlock: settlementBlock || null,
    eIndicatorCustomerInstallment: indicatorCustomerInstallment || null,
    eBank: bank || null,
    eBranch: branch || null,
    eAccount: account || null,
    eAccountDigit: accountDigit || null,
    eArn: arn || null,
    eIndicatorReceivableNegotiation: indicatorReceivableNegotiation || null,
    eCaptureType: captureType || null,
    eNegotiatorDocument: negotiatorDocument || null,
    eCieloUsage2: cieloUsage2 || null,
    eFileHeaderDate: headerDate,
    eRawLine: rawLine,
  };
};

const mapAdjustmentProduct = (code: string): string | null => {
  const normalized = code.trim();
  if (!normalized) {
    return null;
  }
  switch (normalized) {
    case '01':
      return 'DÉBITO';
    case '02':
      return 'CRÉDITO';
    default:
      return normalized;
  }
};

const parseEdiAdjustmentLine = (type: EdiFileType, line: string): EdiAdjustment => {
  const establishmentNo = sliceText(line, 2, 11);
  const nsuDoc = sliceText(line, 12, 26);
  const nsuRelated = sliceText(line, 27, 41);
  const nsuAdjustment = sliceText(line, 42, 56);
  const adjustmentType = sliceText(line, 57, 59);
  const reasonCode = sliceText(line, 60, 62);
  const adjustmentDate = sliceText(line, 63, 70);
  const adjustmentTime = sliceText(line, 71, 75);
  const grossSign = sliceRaw(line, 72, 72);
  const grossDigits = sliceRaw(line, 73, 85);
  const feeSign = sliceRaw(line, 86, 86);
  const feeDigits = sliceRaw(line, 87, 99);
  const netSign = sliceRaw(line, 100, 100);
  const netDigits = sliceRaw(line, 101, 113);
  const brandCode = sliceText(line, 156, 158);
  const productCode = sliceText(line, 159, 160);
  const creditDate = sliceText(line, 161, 168);

  const grossAmount = parseSignedMoney(grossSign, grossDigits).amount;
  const feeAmount = parseSignedMoney(feeSign, feeDigits).amount;
  const netAmount = parseSignedMoney(netSign, netDigits).amount;
  const adjustmentDatetime = parseDateTime(adjustmentDate, adjustmentTime);
  const settlementDate = parseDateTime(creditDate, '000000');
  const brand = mapBrand(brandCode);
  const paymentMethod = mapAdjustmentProduct(productCode);
  const entryParts: string[] = [];
  if (nsuRelated) {
    entryParts.push(`REL=${nsuRelated}`);
  }
  if (nsuAdjustment) {
    entryParts.push(`AJ=${nsuAdjustment}`);
  }
  if (adjustmentType) {
    entryParts.push(`TIPO=${adjustmentType}`);
  }
  const entryType = entryParts.length ? entryParts.join('|') : null;

  return {
    adjustmentDatetime,
    establishmentNo: establishmentNo || null,
    paymentMethod,
    brand,
    grossAmount,
    feeAmount,
    netAmount,
    entryType,
    reason: reasonCode || null,
    settlementDate,
    authCode: null,
    nsuDoc: nsuDoc || null,
    rowHash: buildRowHash(type, line),
  };
};

const parseCielo04RecordD = (
  line: string,
  lineNo: number,
  verbose?: boolean,
): EdiAdjustment => {
  const establishmentNo = sliceText(line, 2, 11);
  const nsuDoc = sliceText(line, 12, 26);
  const nsuRelated = sliceText(line, 27, 41);
  const nsuAdjustment = sliceText(line, 42, 56);
  const adjustmentType = sliceText(line, 57, 59);
  const reasonCode = sliceText(line, 60, 62);
  const adjustmentDate = sliceText(line, 63, 70);
  const adjustmentTime = sliceText(line, 71, 75);
  const grossSign = sliceRaw(line, 72, 72);
  const grossDigits = sliceRaw(line, 73, 85);
  const feeSign = sliceRaw(line, 86, 86);
  const feeDigits = sliceRaw(line, 87, 99);
  const netSign = sliceRaw(line, 100, 100);
  const netDigits = sliceRaw(line, 101, 113);
  const nsuOriginal = sliceText(line, 114, 128);
  const brandCode = sliceText(line, 156, 158);
  const productCode = sliceText(line, 159, 160);
  const creditDate = sliceText(line, 161, 168);

  const grossParsed = parseSignedMoney(grossSign, grossDigits, {
    field: 'ADJ_GROSS',
    range: '72-85',
    rawLine: line,
    lineNo,
  });
  const feeParsed = parseSignedMoney(feeSign, feeDigits, {
    field: 'ADJ_FEE',
    range: '86-99',
    rawLine: line,
    lineNo,
  });
  const netParsed = parseSignedMoney(netSign, netDigits, {
    field: 'ADJ_NET',
    range: '100-113',
    rawLine: line,
    lineNo,
  });
  if (verbose || isDebugEnabled()) {
    logger.debug(
      {
        lineNo,
        ranges: {
          gross: { sign: '72-72', digits: '73-85' },
          fee: { sign: '86-86', digits: '87-99' },
          net: { sign: '100-100', digits: '101-113' },
        },
        gross: { sign: grossSign.trim(), digits: grossDigits.trim(), parsed: grossParsed.amount },
        fee: { sign: feeSign.trim(), digits: feeDigits.trim(), parsed: feeParsed.amount },
        net: { sign: netSign.trim(), digits: netDigits.trim(), parsed: netParsed.amount },
      },
      'CIELO04D ajuste valores',
    );
  }
  const adjustmentDatetime = parseDateTime(adjustmentDate, adjustmentTime);
  const settlementDate = parseDateTime(creditDate, '000000');
  const referenceDate = parseDateYYYYMMDD(adjustmentDate);
  const brand = mapBrand(brandCode);
  const paymentMethod = mapAdjustmentProduct(productCode);

  const entryParts: string[] = [];
  if (nsuRelated) {
    entryParts.push(`REL=${nsuRelated}`);
  }
  if (nsuAdjustment) {
    entryParts.push(`AJ=${nsuAdjustment}`);
  }
  if (adjustmentType) {
    entryParts.push(`TIPO=${adjustmentType}`);
  }
  const entryType = entryParts.length ? entryParts.join('|') : null;

  return {
    adjustmentDatetime,
    establishmentNo: establishmentNo || null,
    paymentMethod,
    brand,
    grossAmount: grossParsed.amount,
    feeAmount: feeParsed.amount,
    netAmount: netParsed.amount,
    entryType,
    reason: reasonCode || null,
    settlementDate,
    authCode: null,
    nsuDoc: nsuDoc || null,
    rowHash: buildCielo04RowHash('D', line),
    lineNo,
    ec: establishmentNo || null,
    adjTypeCode: adjustmentType || null,
    adjReasonCode: reasonCode || null,
    adjGrossSign: grossParsed.sign,
    adjGross: grossParsed.amount,
    adjFeeSign: feeParsed.sign,
    adjFee: feeParsed.amount,
    adjNetSign: netParsed.sign,
    adjNet: netParsed.amount,
    referenceDate,
    urKey: nsuRelated || null,
    nsuOriginal: nsuOriginal || null,
    rawLine: line.trimEnd(),
  } as EdiAdjustment;
};

const parseCielo04ItemFromTransaction = (
  line: string,
  transaction: EdiTransaction,
  lineNo: number,
): EdiAdjustmentItem => {
  const saleDate =
    transaction.eAuthDate ??
    transaction.eCaptureDate ??
    transaction.eEntryDate ??
    (transaction.saleDatetime ? transaction.saleDatetime.slice(0, 10) : null);
  const saleTime = formatTimeFromHHMMSS(transaction.eTimeHhmmss ?? null);
  const brandCode = transaction.eAuthBrand ?? transaction.eLiquidationBrand ?? null;
  const productCode =
    transaction.eTransactionType ?? transaction.eLiquidationType ?? null;

  return {
    lineNo,
    ec: transaction.eSubmitEstablishment ?? transaction.establishmentNo ?? null,
    nsuCielo: transaction.nsuDoc ?? transaction.eNsuDoc ?? null,
    authCode: transaction.authCode ?? transaction.eAuthCode ?? null,
    saleDate,
    saleTime,
    brandCode,
    productCode,
    cardBin: transaction.eCardBin ?? null,
    cardLast4: transaction.eCardLast4 ?? null,
    urKey: transaction.eChaveUr ?? null,
    gross: transaction.grossAmount ?? null,
    net: transaction.netAmount ?? null,
    fee: transaction.feeAmount ?? null,
    rawLine: line.trimEnd(),
    rowHash: buildCielo04RowHash('E', line),
  };
};

export const parseEdiFile = (
  type: EdiFileType,
  content: string,
  filename?: string,
  verbose?: boolean,
): ParsedEdiFile => {
  const lines = content.split(/\r?\n/).filter((line) => line.trim() !== '');
  const delimiter = lines.length > 0 ? detectDelimiter(lines[0]) : null;

  const records = lines.map((line) => {
    if (!delimiter) {
      return [line];
    }
    return line.split(delimiter);
  });

  const headerLine = lines[0] ?? '';
  const headerDate = headerLine ? parseHeaderDate(headerLine) : null;

  const transactions: EdiTransaction[] = [];
  const adjustments: EdiAdjustment[] = [];
  if (type === 'CIELO03') {
    for (const line of lines) {
      if (!line.startsWith('E')) {
        continue;
      }
      transactions.push(parseEdiTransactionLine(type, line, headerDate, filename, verbose));
    }
  }
  if (type === 'CIELO04') {
    for (const line of lines) {
      if (line.startsWith('E')) {
        transactions.push(parseEdiTransactionLine(type, line, headerDate, filename, verbose));
        continue;
      }
      if (line.startsWith('D')) {
        adjustments.push(parseEdiAdjustmentLine(type, line));
      }
    }
  }

  return {
    records,
    summary: {
      type,
      lines: lines.length,
      delimiter,
    },
    transactions,
    adjustments,
    headerDate,
  };
};

export const parseCielo04File = (
  content: string,
  filename?: string,
  verbose?: boolean,
): ParsedCielo04File => {
  const lines = content.split(/\r?\n/).filter((line) => line.trim() !== '');
  const headerLine = lines[0] ?? '';
  const headerDate = headerLine ? parseHeaderDate(headerLine) : null;
  const transactions: EdiTransaction[] = [];
  const adjustmentBlocks: Cielo04AdjustmentBlock[] = [];
  let currentBlock: Cielo04AdjustmentBlock | null = null;
  let loggedAdjustment = false;

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    if (!line) {
      continue;
    }
    const lineNo = index + 1;
    if (line.startsWith('D')) {
      if (currentBlock) {
        adjustmentBlocks.push(currentBlock);
      }
      const shouldLog = Boolean(verbose && !loggedAdjustment);
      currentBlock = {
        adjustment: parseCielo04RecordD(line, lineNo, shouldLog),
        items: [],
      };
      if (shouldLog) {
        loggedAdjustment = true;
      }
      continue;
    }
    if (line.startsWith('E')) {
      const transaction = parseEdiTransactionLine('CIELO04', line, headerDate, filename, verbose);
      transactions.push(transaction);
      if (!currentBlock) {
      if (verbose || isDebugEnabled()) {
        logger.warn(
          { filename, lineNo, linePreview: line.slice(0, 120) },
          'CIELO04D E without D block',
        );
      }
        continue;
      }
      currentBlock.items.push(parseCielo04ItemFromTransaction(line, transaction, lineNo));
    }
  }
  if (currentBlock) {
    adjustmentBlocks.push(currentBlock);
  }

  return { headerDate, transactions, adjustmentBlocks };
};

export const parseCielo16File = (
  content: string,
  filename?: string,
  verbose?: boolean,
): ParsedCielo16File => {
  const lines = content.split(/\r?\n/).filter((line) => line.trim() !== '');
  const headerLine = lines[0] ?? '';
  const header = parseCielo16Header(headerLine);
  const transactions: EdiTransaction[] = [];
  for (const line of lines) {
    if (!line.startsWith('8')) {
      continue;
    }
    const parsed = parseCielo16Detail(line, header.headerDate, header.establishmentNo, verbose);
    if (parsed) {
      transactions.push(parsed);
    }
  }
  const trailerLine = lines.length > 1 && lines[lines.length - 1].startsWith('9')
    ? lines[lines.length - 1]
    : null;
  return {
    headerDate: header.headerDate,
    headerEstablishment: header.establishmentNo,
    transactions,
    headerLine: headerLine || null,
    trailerLine,
  };
};
