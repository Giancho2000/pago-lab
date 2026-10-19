import { contractErrors } from '../../../test/support/asyncapi.js';
import { Payment } from './payment.js';
import { buildPaymentEvent } from './payment-events.js';
import { PaymentStatus as S } from './payment-status.js';

const payment = (status: S, overrides: Partial<Payment> = {}): Payment => ({
  id: '7c1e2a9b-5d4f-4e3a-8b2c-1d0e9f8a7b6c',
  merchantId: 'merchant_demo',
  amountCents: 15_000,
  currency: 'COP',
  description: 'Pedido 1001',
  status,
  gatewayRef: 'ch_1',
  attempts: 1,
  createdAt: '2026-10-15T20:00:00.000Z',
  updatedAt: '2026-10-15T20:00:01.234Z',
  ...overrides,
});

describe('buildPaymentEvent (R12, R13)', () => {
  it.each([
    [S.APPROVED, 'payment.approved'],
    [S.DECLINED, 'payment.declined'],
    [S.FAILED, 'payment.failed'],
    [S.UNKNOWN, 'payment.unknown'],
  ])('%s → %s y cumple el contrato de asyncapi.yaml', (status, type) => {
    const event = buildPaymentEvent(payment(status));

    expect(event).toMatchObject({ type, version: 1 });
    expect(contractErrors(event)).toEqual([]);
  });

  it.each([S.PENDING, S.PROCESSING])('%s no genera evento', (status) => {
    expect(buildPaymentEvent(payment(status))).toBeNull();
  });

  it('occurredAt es el instante de la transición registrado en la BD', () => {
    expect(buildPaymentEvent(payment(S.APPROVED))?.occurredAt).toBe('2026-10-15T20:00:01.234Z');
  });

  it('cada evento tiene un eventId propio, aunque sea del mismo pago', () => {
    const a = buildPaymentEvent(payment(S.UNKNOWN));
    const b = buildPaymentEvent(payment(S.UNKNOWN));
    expect(a?.eventId).not.toBe(b?.eventId);
  });

  it('solo expone los campos del contrato: nada de description, attempts ni fechas internas', () => {
    expect(Object.keys(buildPaymentEvent(payment(S.APPROVED))!.data).sort()).toEqual(
      ['amountCents', 'currency', 'gatewayRef', 'id', 'merchantId', 'status'],
    );
  });

  it('gatewayRef null también cumple el contrato (UNKNOWN sin referencia de la pasarela)', () => {
    const event = buildPaymentEvent(payment(S.UNKNOWN, { gatewayRef: null }));
    expect(contractErrors(event)).toEqual([]);
  });

  it('el validador detecta un evento fuera de contrato', () => {
    const event = { ...buildPaymentEvent(payment(S.APPROVED))!, type: 'payment.rejected', occurredAt: 'Thu Oct 15 2026' };
    expect(contractErrors(event)).toHaveLength(2);
  });
});
