import { createHmac } from 'node:crypto';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Pool } from 'pg';
import { deliver, NonRetryableError, parseEvent, type PaymentEvent } from '../src/deliver.js';
import { isRetryable } from '../src/retry.js';

const CFG = { url: 'http://merchant.test/webhooks', secret: 'whsec_test' };
const EVENT: PaymentEvent = {
  eventId: '0b8f3c1e-6a2d-4f5b-9c7e-1d2a3b4c5d6e',
  type: 'payment.approved',
  data: { id: '7c9e6679-7425-40de-944b-e07fc1f90ae7' },
};

// Pool falso: la primera query es el SELECT de deduplicación y la segunda el INSERT
function fakePool(alreadyDelivered = false) {
  const query = vi.fn()
    .mockResolvedValueOnce({ rowCount: alreadyDelivered ? 1 : 0 })
    .mockResolvedValueOnce({ rowCount: 1 });
  return { pool: { query } as unknown as Pool, query };
}

const respond = (status: number) => vi.fn().mockResolvedValue(new Response(null, { status }));

describe('parseEvent', () => {
  it('devuelve el evento si es JSON con eventId UUID y data.id', () => {
    expect(parseEvent(Buffer.from(JSON.stringify(EVENT)))).toEqual(EVENT);
  });

  it.each([
    ['texto que no es JSON', Buffer.from('esto no es json'), 'el mensaje no es JSON'],
    ['mensaje vacío (tombstone)', null, 'el mensaje no es JSON'],
    ['eventId que no es UUID', Buffer.from('{"eventId":"e1","data":{"id":"p1"}}'), 'sin eventId'],
    ['sin data.id', Buffer.from(`{"eventId":"${EVENT.eventId}","data":{}}`), 'sin data.id'],
  ])('%s → NonRetryableError (va directo al DLT)', (_, value, message) => {
    const parse = () => parseEvent(value);
    expect(parse).toThrow(NonRetryableError);
    expect(parse).toThrow(message);
  });
});

describe('deliver', () => {
  let fetchMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    fetchMock = respond(200);
    vi.stubGlobal('fetch', fetchMock);
  });
  afterEach(() => vi.unstubAllGlobals());

  it('si el evento ya está en notifier.deliveries no lo reenvía', async () => {
    const { pool, query } = fakePool(true);

    await expect(deliver(pool, EVENT, CFG)).resolves.toBe('duplicate');
    expect(fetchMock).not.toHaveBeenCalled();
    expect(query).toHaveBeenCalledTimes(1);
  });

  it('2xx → POST firmado que el receptor puede verificar, y registra la entrega', async () => {
    const { pool, query } = fakePool();

    await expect(deliver(pool, EVENT, CFG)).resolves.toBe('delivered');

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe(CFG.url);
    expect(init.method).toBe('POST');
    expect(init.body).toBe(JSON.stringify(EVENT));

    // Verifica la firma como lo hace merchant-sim: HMAC de "<t>.<cuerpo crudo>"
    const [, t, v1] = init.headers['pagolab-signature'].match(/^t=(\d+),v1=([0-9a-f]{64})$/);
    expect(Math.abs(Date.now() / 1000 - Number(t))).toBeLessThan(5);
    expect(v1).toBe(createHmac('sha256', CFG.secret).update(`${t}.${init.body}`).digest('hex'));

    expect(query).toHaveBeenLastCalledWith(expect.stringContaining('INSERT INTO notifier.deliveries'), [EVENT.eventId, EVENT.data.id]);
  });

  it.each([400, 401])('%i → NonRetryableError y no registra la entrega', async (status) => {
    vi.stubGlobal('fetch', respond(status));
    const { pool, query } = fakePool();

    const err = await deliver(pool, EVENT, CFG).catch((e) => e);
    expect(err).toBeInstanceOf(NonRetryableError);
    expect(err.message).toBe(`rechazo ${status}`);
    expect(isRetryable(err)).toBe(false);
    expect(query).toHaveBeenCalledTimes(1);
  });

  it.each([500, 503, 403, 404, 422])('%i → error reintentable y no registra la entrega', async (status) => {
    vi.stubGlobal('fetch', respond(status));
    const { pool, query } = fakePool();

    const err = await deliver(pool, EVENT, CFG).catch((e) => e);
    expect(err).not.toBeInstanceOf(NonRetryableError);
    expect(err.message).toBe(`webhook ${status}`);
    expect(isRetryable(err)).toBe(true);
    expect(query).toHaveBeenCalledTimes(1);
  });

  it('timeout o error de red → error reintentable', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new DOMException('The operation was aborted', 'TimeoutError')));
    const { pool, query } = fakePool();

    const err = await deliver(pool, EVENT, CFG).catch((e) => e);
    expect(isRetryable(err)).toBe(true);
    expect(query).toHaveBeenCalledTimes(1);
  });

  it('si falla el INSERT tras un 2xx, el error es reintentable (el comercio deduplica el reenvío)', async () => {
    const query = vi.fn()
      .mockResolvedValueOnce({ rowCount: 0 })
      .mockRejectedValueOnce(new Error('connection terminated'));

    const err = await deliver({ query } as unknown as Pool, EVENT, CFG).catch((e) => e);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(isRetryable(err)).toBe(true);
  });
});
