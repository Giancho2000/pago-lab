import { INestApplication } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { Pool } from 'pg';
import request from 'supertest';
import { MerchantRateLimiter } from '../src/common/rate-limit/merchant-rate-limiter.js';
import { createTestApp, truncateAll } from './support/app.js';
import { FakeGateway } from './support/fake-gateway.js';

const BURST = 3;
const PER_SECOND = 2; // un token cada 500 ms: margen amplio frente a lo que tarda cada petición

describe('Rate limit por comercio (e2e)', () => {
  const gateway = new FakeGateway();
  let app: INestApplication;
  let pool: Pool;
  let merchant: string;

  const createPayment = (opts: { merchantId?: string; key?: string } = {}) =>
    request(app.getHttpServer())
      .post('/payments')
      .set('X-Merchant-Id', opts.merchantId ?? merchant)
      .set('Idempotency-Key', opts.key ?? randomUUID())
      .send({ amountCents: 1_000, currency: 'COP' });

  const drain = async (n = BURST) => {
    for (let i = 0; i < n; i++) await createPayment().expect(201);
  };

  beforeAll(async () => {
    await gateway.start();
    ({ app, pool } = await createTestApp(gateway, {
      RATE_LIMIT_ENABLED: 'true',
      RATE_LIMIT_BURST: String(BURST),
      RATE_LIMIT_PER_SECOND: String(PER_SECOND),
    }));
  });

  beforeEach(async () => {
    merchant = `m_${randomUUID()}`;
    gateway.reset();
    await truncateAll(pool);
  });

  afterAll(async () => {
    await app?.close();
    await gateway.stop();
  });

  it('informa el límite y lo que queda en cada creación', async () => {
    const first = await createPayment().expect(201);
    expect(first.headers['ratelimit-limit']).toBe(String(BURST));
    expect(first.headers['ratelimit-remaining']).toBe(String(BURST - 1));

    const second = await createPayment().expect(201);
    expect(second.headers['ratelimit-remaining']).toBe(String(BURST - 2));
  });

  it('agotada la ráfaga → 429 problem+json con Retry-After, sin cobrar ni reclamar la clave', async () => {
    await drain();
    const key = randomUUID();

    const res = await createPayment({ key }).expect(429);

    expect(res.headers['content-type']).toContain('application/problem+json');
    expect(res.headers['retry-after']).toBe('1');
    expect(res.headers['ratelimit-remaining']).toBe('0');
    expect(res.body).toMatchObject({ status: 429, title: 'TOO_MANY_REQUESTS', instance: '/payments' });
    expect(gateway.chargeCalls).toHaveLength(BURST); // el rechazado nunca llegó a la pasarela

    const { rowCount } = await pool.query(`SELECT 1 FROM idempotency_keys WHERE key = $1`, [key]);
    expect(rowCount).toBe(0); // reintentar con la misma clave es seguro
  });

  it('los tokens se recargan con el tiempo', async () => {
    await drain();
    await createPayment().expect(429);

    await sleep(1_000 / PER_SECOND + 50); // algo más de lo que tarda en recargarse un token
    await createPayment().expect(201);
    await createPayment().expect(429);
  });

  it('cada comercio tiene su propio balde', async () => {
    await drain();
    await createPayment().expect(429);

    await createPayment({ merchantId: `m_${randomUUID()}` }).expect(201);
  });

  it('peticiones concurrentes nunca gastan el mismo token', async () => {
    const limiter = app.get(MerchantRateLimiter);

    // 20 consumos simultáneos del mismo comercio: solo pasan BURST más lo que alcance a recargarse mientras tanto
    const started = Date.now();
    const decisions = await Promise.all(Array.from({ length: 20 }, () => limiter.consume(merchant)));
    const refilled = Math.ceil(((Date.now() - started) / 1_000) * PER_SECOND);
    const allowed = decisions.filter((d) => d.allowed).length;

    expect(allowed).toBeGreaterThanOrEqual(BURST);
    expect(allowed).toBeLessThanOrEqual(BURST + refilled);
  });

  it('el reintento tras el 429 con la misma clave crea el pago una sola vez', async () => {
    await drain();
    const key = randomUUID();
    await createPayment({ key }).expect(429);

    await sleep(1_000 / PER_SECOND + 50);
    const created = await createPayment({ key }).expect(201);

    await sleep(1_000 / PER_SECOND + 50);
    const replay = await createPayment({ key }).expect(201);
    expect(replay.headers['idempotent-replayed']).toBe('true');
    expect(replay.body.id).toBe(created.body.id);
  });
});
