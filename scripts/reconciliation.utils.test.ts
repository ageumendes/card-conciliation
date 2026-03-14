import * as assert from 'assert';
import {
  amountDiff,
  AMOUNT_TOLERANCE,
  TIME_TOLERANCE_MS,
  buildTimeWindow,
  diffSeconds,
  buildDayWindow,
  hasTimeComponent,
  isCancelledStatus,
  isSameDay,
  isWithinAmountTolerance,
  isWithinTimeTolerance,
  normalizePaymentDetails,
  normalizePaymentDetailsFromValues,
  normalizeBrand,
  normalizeIdentifier,
  normalizeMethodToCreditDebit,
  parseBoolean,
  stripLeadingZeros,
  toFbDateString,
  toFbTimestampString,
} from '../src/modules/reconciliation/reconciliation.utils';

const run = () => {
  assert.strictEqual(normalizeIdentifier(' 12 34 '), '1234');
  assert.strictEqual(stripLeadingZeros('000123'), '123');
  assert.strictEqual(normalizeMethodToCreditDebit('credito'), 'CREDIT');
  assert.strictEqual(normalizeMethodToCreditDebit('debito'), 'DEBIT');
  assert.strictEqual(normalizeMethodToCreditDebit('parcelado'), 'CREDIT');
  assert.strictEqual(normalizeBrand(' Visa Electron '), 'VISA');
  assert.strictEqual(normalizeBrand('Mastercard'), 'MASTER');
  assert.strictEqual(isCancelledStatus('Cancelado'), true);
  assert.strictEqual(isCancelledStatus('Aprovado'), false);
  assert.strictEqual(amountDiff(10.0, 9.99), 0.01);

  const left = new Date('2026-01-05T23:59:30');
  const right = new Date('2026-01-06T00:01:00');
  assert.strictEqual(diffSeconds(left, right), 90);
  assert.strictEqual(isSameDay(left, right), false);

  const stamp = toFbTimestampString(new Date('2026-01-05T08:07:06'));
  assert.strictEqual(stamp, '2026-01-05 08:07:06');
  const dateOnly = toFbDateString(new Date('2026-01-05T23:59:59'));
  assert.strictEqual(dateOnly, '2026-01-05');

  assert.strictEqual(parseBoolean('true'), true);
  assert.strictEqual(parseBoolean('1'), true);
  assert.strictEqual(parseBoolean('yes'), true);
  assert.strictEqual(parseBoolean('false'), false);
  assert.strictEqual(parseBoolean('0'), false);
  assert.strictEqual(parseBoolean(''), false);

  const acqDate1 = new Date('2026-01-09T17:43:00');
  const erpDate1 = new Date('2026-01-09T17:02:00');
  assert.strictEqual(isWithinTimeTolerance(acqDate1, erpDate1), true);

  const acqDate2 = new Date('2026-01-09T14:00:00');
  const erpDate2 = new Date('2026-01-09T13:02:00');
  assert.strictEqual(isWithinTimeTolerance(acqDate2, erpDate2), true);

  const window = buildTimeWindow(acqDate1);
  assert.strictEqual(acqDate1.getTime() - window.start.getTime(), TIME_TOLERANCE_MS);
  assert.strictEqual(window.end.getTime() - acqDate1.getTime(), TIME_TOLERANCE_MS);

  assert.strictEqual(isWithinAmountTolerance(17.48, 17.47), true);
  assert.strictEqual(isWithinAmountTolerance(17.48, 17.48 + AMOUNT_TOLERANCE + 0.01), false);

  assert.deepStrictEqual(normalizePaymentDetails('PIX'), { paymentMethod: 'PIX', cardType: 'UNKNOWN' });
  assert.deepStrictEqual(normalizePaymentDetails('CARTEIRA DIGITAL'), {
    paymentMethod: 'PIX',
    cardType: 'UNKNOWN',
  });
  assert.deepStrictEqual(normalizePaymentDetails('DEBITO'), { paymentMethod: 'CARD', cardType: 'DEBIT' });
  assert.deepStrictEqual(normalizePaymentDetails('CREDITO'), {
    paymentMethod: 'CARD',
    cardType: 'CREDIT',
  });
  assert.deepStrictEqual(normalizePaymentDetailsFromValues('', 'CARTAO'), {
    paymentMethod: 'CARD',
    cardType: 'UNKNOWN',
  });

  const interDate = new Date('2026-01-05T00:00:00');
  const acqDate = new Date('2026-01-05T18:43:00');
  assert.strictEqual(hasTimeComponent(interDate), false);
  assert.strictEqual(hasTimeComponent(acqDate), true);
  const dayWindow = buildDayWindow(interDate);
  assert.strictEqual(dayWindow.start.getHours(), 0);
  assert.strictEqual(dayWindow.end.getHours(), 23);
  assert.strictEqual(isWithinTimeTolerance(interDate, acqDate), false);

  console.log('Reconciliation utils tests passed');
};

run();
