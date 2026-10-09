import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import request from 'supertest';
import { AppModule } from '../src/app.module.js';
import { configureApp } from '../src/app.setup.js';
import { PG_POOL } from '../src/database/database.module.js';
import { PaymentStatus as S } from '../src/payments/domain/payment-status.js';
import { GatewayClient } from '../src/payments/gateway.client.js';
import { PaymentsRepository } from '../src/payments/payments.repository.js';
import { UnknownPaymentsResolver } from '../src/payments/unknown-payments.resolver.js';
import { runMigrations } from './helpers.js';

describe('Pagos (integración)', () => {
  let container: StartedPostgreSqlContainer;
  let app: INestApplication;
  const gateway = { charge: vitest.fn(), getCharge: vitest.fn() };
  const merchant = 'merchant_test';
  const body = { amountCents: 150000, currency: 'COP', description: 'Prueba' };

  beforeAll(async () => {
    container = await new PostgreSqlContainer('postgres:17-alpine').start();
    process.env.DATABASE_URL = container.getConnectionUri();
    process.env.RESOLVER_ENABLED = 'false';

    const moduleRef = await Test.createTestingModule({ imports: [AppModule] })
      .overrideProvider(GatewayClient) // mock de lo externo; base de datos real
      .useValue(gateway)
      .compile();

    app = moduleRef.createNestApplication();
    configureApp(app);
    await app.init();
    await runMigrations(app.get<Pool>(PG_POOL));
  });

  afterAll(async () => {
    await app.close();
    await container.stop();
  });

  beforeEach(() => vitest.resetAllMocks());

  const post = (key: string, payload: object = body) =>
    request(app.getHttpServer())
      .post('/payments')
      .set('X-Merchant-Id', merchant)
      .set('Idempotency-Key', key)
      .send(payload);

  it('cobra y responde 201 APPROVED', async () => {
    gateway.charge.mockResolvedValue({ outcome: S.APPROVED, gatewayRef: 'ch_1' });
    const res = await post(randomUUID());
    expect(res.status).toBe(201);
    expect(res.body.status).toBe(S.APPROVED);
    expect(res.headers.location).toBe(`/payments/${res.body.id}`);
  });

  it('repite la respuesta sin cobrar dos veces', async () => {
    gateway.charge.mockResolvedValue({ outcome: S.APPROVED, gatewayRef: 'ch_2' });
    const key = randomUUID();
    const first = await post(key);
    const second = await post(key);
    expect(second.status).toBe(201);
    expect(second.headers['idempotent-replayed']).toBe('true');
    expect(second.body.id).toBe(first.body.id);
    expect(gateway.charge).toHaveBeenCalledTimes(1);
  });

  it('responde 422 si la misma clave llega con otro cuerpo', async () => {
    gateway.charge.mockResolvedValue({ outcome: S.APPROVED, gatewayRef: 'ch_3' });
    const key = randomUUID();
    await post(key);
    const res = await post(key, { ...body, amountCents: 999 });
    expect(res.status).toBe(422);
    expect(res.headers['content-type']).toContain('application/problem+json');
  });

  it('responde 409 si la petición original sigue en proceso', async () => {
    gateway.charge.mockImplementation(
      () => new Promise((r) => setTimeout(() => r({ outcome: S.APPROVED, gatewayRef: 'ch_4' }), 500)),
    );
    const key = randomUUID();
    const [a, b] = await Promise.all([post(key), post(key)]);
    expect([a.status, b.status].sort()).toEqual([201, 409]);
  });

  it('valida headers y cuerpo', async () => {
    const sinClave = await request(app.getHttpServer())
      .post('/payments').set('X-Merchant-Id', merchant).send(body);
    expect(sinClave.status).toBe(400);
    const sinComercio = await request(app.getHttpServer())
      .post('/payments').set('Idempotency-Key', randomUUID()).send(body);
    expect(sinComercio.status).toBe(401);
    const campoExtra = await post(randomUUID(), { ...body, hack: true });
    expect(campoExtra.status).toBe(400);
  });

  it('timeout → 202 UNKNOWN y el resolver lo aprueba', async () => {
    gateway.charge.mockResolvedValue({ outcome: S.UNKNOWN, reason: 'TimeoutError' });
    const created = await post(randomUUID());
    expect(created.status).toBe(202);
    expect(created.body.status).toBe(S.UNKNOWN);

    gateway.getCharge.mockResolvedValue({ found: true, status: 'APPROVED', gatewayRef: 'ch_5' });
    await app.get(UnknownPaymentsResolver).runOnce();

    const res = await request(app.getHttpServer())
      .get(`/payments/${created.body.id}`).set('X-Merchant-Id', merchant);
    expect(res.body.status).toBe(S.APPROVED);
  });

  it('no permite consultar pagos de otro comercio', async () => {
    gateway.charge.mockResolvedValue({ outcome: S.APPROVED, gatewayRef: 'ch_6' });
    const created = await post(randomUUID());
    const res = await request(app.getHttpServer())
      .get(`/payments/${created.body.id}`).set('X-Merchant-Id', 'otro');
    expect(res.status).toBe(404);
  });

  it('solo un proceso gana la misma transición', async () => {
    const repo = app.get(PaymentsRepository);
    const p = await repo.insert({ merchantId: merchant, amountCents: 100, currency: 'COP' });
    const results = await Promise.all([
      repo.transition(p.id, S.PENDING, S.PROCESSING),
      repo.transition(p.id, S.PENDING, S.PROCESSING),
    ]);
    expect(results.filter(Boolean)).toHaveLength(1);
  });

  it('health ready responde 200', async () => {
    await request(app.getHttpServer()).get('/health/ready').expect(200);
  });
});