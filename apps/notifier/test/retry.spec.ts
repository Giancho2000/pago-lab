import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Producer } from 'kafkajs';
import { NonRetryableError } from '../src/deliver.js';
import { isRetryable, sendToRetryOrDlt } from '../src/retry.js';

const NOW = 1_760_900_000_000;

function fakeProducer() {
  const send = vi.fn().mockResolvedValue([]);
  return { producer: { send } as unknown as Producer, send };
}

const message = (headers: Record<string, string> = {}) => ({
  key: Buffer.from('pay-1'),
  value: Buffer.from('{"eventId":"e1"}'),
  headers: { 'event-id': 'e1', 'event-type': 'payment.approved', ...headers },
});

describe('sendToRetryOrDlt', () => {
  beforeEach(() => { vi.useFakeTimers(); vi.setSystemTime(NOW); });
  afterEach(() => vi.useRealTimers());

  it.each([
    [undefined, '1', 5_000],
    ['1', '2', 30_000],
    ['2', '3', 120_000],
  ])('reintentable con attempt previo %s → retry con attempt %s y espera de %i ms', async (prev, attempt, delay) => {
    const { producer, send } = fakeProducer();
    const result = await sendToRetryOrDlt(producer, message(prev ? { attempt: prev } : {}), new Error('webhook 500'), true);

    const sent = send.mock.calls[0][0];
    expect(result).toBe('retry');
    expect(sent.topic).toBe('payments.events.retry');
    expect(sent.messages[0].headers).toMatchObject({ attempt, 'not-before': String(NOW + delay) });
  });

  it('el 4.º fallo va al DLT con la causa en last-error', async () => {
    const { producer, send } = fakeProducer();
    const result = await sendToRetryOrDlt(producer, message({ attempt: '3' }), new Error('webhook 500'), true);

    expect(result).toBe('dlt');
    expect(send.mock.calls[0][0].topic).toBe('payments.events.dlt');
    expect(send.mock.calls[0][0].messages[0].headers).toMatchObject({ attempt: '4', 'last-error': 'webhook 500' });
  });

  it('un error no reintentable va directo al DLT en el primer intento', async () => {
    const { producer, send } = fakeProducer();
    const err = new NonRetryableError('rechazo 401');

    expect(isRetryable(err)).toBe(false);
    await sendToRetryOrDlt(producer, message(), err, isRetryable(err));
    expect(send.mock.calls[0][0].topic).toBe('payments.events.dlt');
  });

  it('conserva clave, valor y cabeceras originales; last-error se trunca a 500', async () => {
    const { producer, send } = fakeProducer();
    await sendToRetryOrDlt(producer, message(), new Error('x'.repeat(800)), true);

    const sent = send.mock.calls[0][0].messages[0];
    expect(sent.key.toString()).toBe('pay-1');
    expect(sent.value.toString()).toBe('{"eventId":"e1"}');
    expect(sent.headers).toMatchObject({ 'event-id': 'e1', 'event-type': 'payment.approved', 'original-topic': 'payments.events' });
    expect(sent.headers['last-error']).toHaveLength(500);
  });
});