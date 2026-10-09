import { INestApplication } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import request from 'supertest';
import { PaymentsRepository } from '../src/payments/payments.repository.js';
import { PaymentsService } from '../src/payments/payments.service.js';
import { createTestApp, truncateAll } from './support/app.js';
import { FakeGateway } from './support/fake-gateway.js';

describe('Pagos (e2e)', () => {
  const gateway = new FakeGateway();
  let app: INestApplication;
  let pool: Pool;
  let merchant: string;

  const body = {
    amountCents: 15_000,
    currency: 'COP',
    description: 'Pedido #1',
  };

  const createPayment = (
    opts: {
      key?: string | null;
      merchantId?: string | null;
      payload?: object;
    } = {},
  ) => {
    const req = request(app.getHttpServer()).post('/payments');
    if (opts.merchantId !== null)
      req.set('X-Merchant-Id', opts.merchantId ?? merchant);
    if (opts.key !== null) req.set('Idempotency-Key', opts.key ?? randomUUID());
    return req.send(opts.payload ?? body);
  };

  const eventsOf = async (paymentId: string) =>
    (
      await pool.query(
        `SELECT from_status, to_status, reason FROM payment_events WHERE payment_id = $1 ORDER BY id`,
        [paymentId],
      )
    ).rows;

  beforeAll(async () => {
    await gateway.start();
    ({ app, pool } = await createTestApp(gateway));
  });

  beforeEach(async () => {
    merchant = `m_${randomUUID()}`;
    gateway.reset();
    await truncateAll(pool);
  });

  afterEach(() => vitest.restoreAllMocks());

  afterAll(async () => {
    await app?.close();
    await gateway.stop();
  });

  describe('R1 · crear y cobrar', () => {
    it('cobro aprobado → 201 con estado final, Location y gatewayRef', async () => {
      const res = await createPayment().expect(201);

      expect(res.body).toMatchObject({
        merchantId: merchant,
        amountCents: 15_000,
        currency: 'COP',
        description: 'Pedido #1',
        status: 'APPROVED',
        gatewayRef: expect.stringMatching(/^ch_/),
      });
      expect(res.headers.location).toBe(`/payments/${res.body.id}`);
      expect(res.headers['idempotent-replayed']).toBeUndefined();
      expect(gateway.chargeCalls).toEqual([res.body.id]); // nuestro id viaja como clave a la pasarela
    });

    it('cobro rechazado por fondos → 201 DECLINED', async () => {
      gateway.next('decline');
      const res = await createPayment().expect(201);
      expect(res.body.status).toBe('DECLINED');
      expect(await eventsOf(res.body.id)).toContainEqual({
        from_status: 'PROCESSING',
        to_status: 'DECLINED',
        reason: 'fondos insuficientes',
      });
    });

    it('pasarela responde 4xx → 201 FAILED (hay certeza de que no cobró)', async () => {
      gateway.next('reject');
      const res = await createPayment().expect(201);
      expect(res.body.status).toBe('FAILED');
    });

    it('pasarela responde 5xx → 202 UNKNOWN', async () => {
      gateway.next('error');
      const res = await createPayment().expect(202);
      expect(res.body.status).toBe('UNKNOWN');
    });

    it('description es opcional', async () => {
      const res = await createPayment({
        payload: { amountCents: 100, currency: 'USD' },
      }).expect(201);
      expect(res.body.description).toBeNull();
    });

    it.each([
      ['moneda no soportada', { amountCents: 100, currency: 'EUR' }],
      ['monto cero', { amountCents: 0, currency: 'COP' }],
      ['monto decimal', { amountCents: 10.5, currency: 'COP' }],
      [
        'monto sobre el máximo',
        { amountCents: 1_000_000_001, currency: 'COP' },
      ],
      [
        'campo no permitido',
        { amountCents: 100, currency: 'COP', status: 'APPROVED' },
      ],
    ])(
      'cuerpo inválido (%s) → 400 problem+json sin cobrar',
      async (_, payload) => {
        const res = await createPayment({ payload }).expect(400);
        expect(res.headers['content-type']).toContain(
          'application/problem+json',
        );
        expect(res.body).toMatchObject({
          type: 'about:blank',
          title: 'BAD_REQUEST',
          status: 400,
          instance: '/payments',
        });
        expect(gateway.chargeCalls).toHaveLength(0);
      },
    );
  });

  describe('R2 · headers', () => {
    it('sin Idempotency-Key → 400', async () => {
      const res = await createPayment({ key: null }).expect(400);
      expect(res.body.detail).toBe(
        'Idempotency-Key es obligatorio y debe ser un UUID',
      );
    });

    it('Idempotency-Key que no es UUID → 400', async () => {
      await createPayment({ key: 'abc-123' }).expect(400);
    });

    it('sin X-Merchant-Id → 401', async () => {
      const res = await createPayment({ merchantId: null }).expect(401);
      expect(res.body).toMatchObject({
        status: 401,
        detail: 'Falta el header X-Merchant-Id',
      });
    });
  });

  describe('R3 · idempotencia: repetición', () => {
    it('misma clave y mismo cuerpo → misma respuesta, Idempotent-Replayed y sin volver a cobrar', async () => {
      const key = randomUUID();
      const first = await createPayment({ key }).expect(201);
      const replay = await createPayment({ key }).expect(201);

      expect(replay.body).toEqual(first.body);
      expect(replay.headers['idempotent-replayed']).toBe('true');
      expect(replay.headers.location).toBe(first.headers.location);
      expect(gateway.chargeCalls).toHaveLength(1);
      expect(
        (await pool.query('SELECT count(*)::int AS n FROM payments')).rows[0].n,
      ).toBe(1);
    });

    it('la repetición de un 202 UNKNOWN también devuelve 202', async () => {
      const key = randomUUID();
      gateway.next('error');
      await createPayment({ key }).expect(202);
      const replay = await createPayment({ key }).expect(202);
      expect(replay.headers['idempotent-replayed']).toBe('true');
      expect(gateway.chargeCalls).toHaveLength(1);
    });

    it('la misma clave en otro comercio es independiente', async () => {
      const key = randomUUID();
      const a = await createPayment({ key }).expect(201);
      const b = await createPayment({
        key,
        merchantId: `m_${randomUUID()}`,
      }).expect(201);
      expect(b.body.id).not.toBe(a.body.id);
      expect(b.headers['idempotent-replayed']).toBeUndefined();
    });
  });

  describe('R4 · idempotencia: conflictos', () => {
    it('misma clave con otro cuerpo → 422', async () => {
      const key = randomUUID();
      await createPayment({ key }).expect(201);
      const res = await createPayment({
        key,
        payload: { ...body, amountCents: 99 },
      }).expect(422);
      expect(res.body.detail).toBe(
        'La Idempotency-Key ya se usó con otro cuerpo',
      );
      expect(gateway.chargeCalls).toHaveLength(1);
    });

    it('misma clave mientras la original sigue en proceso → 409', async () => {
      const key = randomUUID();
      gateway.next('delay'); // la pasarela tarda, pero responde antes del timeout
      const original = createPayment({ key }).then((r) => r);

      await vitest.waitFor(() => expect(gateway.chargeCalls).toHaveLength(1));
      const res = await createPayment({ key }).expect(409);
      expect(res.body.detail).toMatch(/sigue en proceso/);

      expect((await original).status).toBe(201);
      expect(gateway.chargeCalls).toHaveLength(1);
    });

    it('clave expirada → 422', async () => {
      const key = randomUUID();
      await createPayment({ key }).expect(201);
      await pool.query(
        `UPDATE idempotency_keys SET expires_at = now() - interval '1 second' WHERE key = $1`,
        [key],
      );
      const res = await createPayment({ key }).expect(422);
      expect(res.body.detail).toBe('La Idempotency-Key expiró; usa una nueva');
    });

    it('si falla antes de cobrar, libera la clave para reintentar y responde 500 sin filtrar detalles', async () => {
      const key = randomUUID();
      vitest
        .spyOn(app.get(PaymentsService), 'createAndCharge')
        .mockRejectedValueOnce(new Error('conexión perdida con secreto=123'));

      const res = await createPayment({ key }).expect(500);
      expect(res.body).toMatchObject({
        status: 500,
        title: 'INTERNAL_SERVER_ERROR',
        detail: 'Error interno',
      });
      expect(JSON.stringify(res.body)).not.toContain('secreto');

      const retry = await createPayment({ key }).expect(201);
      expect(retry.headers['idempotent-replayed']).toBeUndefined();
    });
  });

  describe('R5 · máquina de estados', () => {
    it('registra cada transición en payment_events', async () => {
      const res = await createPayment().expect(201);
      expect(await eventsOf(res.body.id)).toEqual([
        {
          from_status: 'PENDING',
          to_status: 'PROCESSING',
          reason: 'inicio de cobro',
        },
        { from_status: 'PROCESSING', to_status: 'APPROVED', reason: null },
      ]);
    });

    it('si otro proceso ya tomó el pago, no cobra y devuelve el estado actual', async () => {
      vitest
        .spyOn(app.get(PaymentsRepository), 'transition')
        .mockResolvedValueOnce(null);
      const res = await createPayment().expect(201);
      expect(res.body.status).toBe('PENDING');
      expect(gateway.chargeCalls).toHaveLength(0);
    });

    it('si otro proceso resolvió el pago durante el cobro, devuelve el estado que dejó', async () => {
      const repo = app.get(PaymentsRepository);
      const real = repo.transition.bind(repo);
      vitest
        .spyOn(repo, 'transition')
        .mockImplementationOnce(real) // PENDING → PROCESSING
        .mockResolvedValueOnce(null); // PROCESSING → APPROVED lo "gana" otro proceso
      const res = await createPayment().expect(201);
      expect(res.body.status).toBe('PROCESSING');
    });
  });

  describe('R6 · timeout de la pasarela', () => {
    it('sin respuesta dentro de GATEWAY_TIMEOUT_MS → 202 UNKNOWN, nunca FAILED', async () => {
      gateway.next('hang'); // la pasarela SÍ cobra, pero responde tarde
      const res = await createPayment().expect(202);

      expect(res.body.status).toBe('UNKNOWN');
      expect(await eventsOf(res.body.id)).toContainEqual({
        from_status: 'PROCESSING',
        to_status: 'UNKNOWN',
        reason: 'TimeoutError',
      });
      expect(gateway.charges.has(res.body.id)).toBe(true); // por eso no puede ser FAILED
    });
  });

  describe('R8 · consulta', () => {
    it('GET /payments/:id del propio comercio → 200', async () => {
      const created = await createPayment().expect(201);
      const res = await request(app.getHttpServer())
        .get(`/payments/${created.body.id}`)
        .set('X-Merchant-Id', merchant)
        .expect(200);
      expect(res.body).toEqual(created.body);
    });

    it('GET /payments/:id de otro comercio → 404', async () => {
      const created = await createPayment().expect(201);
      const res = await request(app.getHttpServer())
        .get(`/payments/${created.body.id}`)
        .set('X-Merchant-Id', `m_${randomUUID()}`)
        .expect(404);
      expect(res.body.detail).toBe('Pago no encontrado');
    });

    it('GET /payments/:id con id que no es UUID → 400', async () => {
      await request(app.getHttpServer())
        .get('/payments/123')
        .set('X-Merchant-Id', merchant)
        .expect(400);
    });

    it('GET /payments/:id sin X-Merchant-Id → 401', async () => {
      await request(app.getHttpServer())
        .get(`/payments/${randomUUID()}`)
        .expect(401);
    });
  });
});
