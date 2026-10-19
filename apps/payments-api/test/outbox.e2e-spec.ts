import { INestApplication } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import request from 'supertest';
import { PaymentStatus } from '../src/payments/domain/payment-status.js';
import { PaymentsRepository } from '../src/payments/payments.repository.js';
import { UnknownPaymentsResolver } from '../src/payments/unknown-payments.resolver.js';
import { createTestApp, truncateAll } from './support/app.js';
import { contractErrors } from './support/asyncapi.js';
import { ChargeBehavior, FakeGateway } from './support/fake-gateway.js';

describe('Transactional outbox (R12)', () => {
  const gateway = new FakeGateway();
  let app: INestApplication;
  let pool: Pool;
  let repo: PaymentsRepository;

  const createPayment = (key = randomUUID()) =>
    request(app.getHttpServer())
      .post('/payments')
      .set('X-Merchant-Id', 'm_outbox')
      .set('Idempotency-Key', key)
      .send({ amountCents: 15_000, currency: 'COP' });

  const outboxOf = async (paymentId: string) =>
    (
      await pool.query(
        `SELECT id, aggregate_id, event_type, payload, published_at
           FROM outbox WHERE aggregate_id = $1 ORDER BY created_at`,
        [paymentId],
      )
    ).rows;

  // Pago ya en PROCESSING, listo para su transición final
  const processing = async () => {
    const payment = await repo.insert({ merchantId: 'm_outbox', amountCents: 100, currency: 'COP' });
    await repo.transition(payment.id, PaymentStatus.PENDING, PaymentStatus.PROCESSING);
    return payment.id;
  };

  beforeAll(async () => {
    await gateway.start();
    ({ app, pool } = await createTestApp(gateway));
    repo = app.get(PaymentsRepository);
  });

  beforeEach(async () => {
    gateway.reset();
    await truncateAll(pool);
  });

  afterAll(async () => {
    await app?.close();
    await gateway.stop();
  });

  it.each<[ChargeBehavior, string, string]>([
    ['approve', 'APPROVED', 'payment.approved'],
    ['decline', 'DECLINED', 'payment.declined'],
    ['reject', 'FAILED', 'payment.failed'],
    ['error', 'UNKNOWN', 'payment.unknown'],
  ])('pasarela %s → %s: exactamente 1 evento %s, pendiente de publicar', async (behavior, status, type) => {
    gateway.next(behavior);
    const res = await createPayment();
    expect(res.body.status).toBe(status);

    const rows = await outboxOf(res.body.id);

    // PENDING → PROCESSING queda en payment_events, pero no genera evento
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ aggregate_id: res.body.id, event_type: type, published_at: null });
    expect(rows[0].payload.eventId).toBe(rows[0].id);
    expect(rows[0].payload.data).toMatchObject({ id: res.body.id, status, amountCents: 15_000 });
    // El payload tal como sale de la BD (jsonb) es el que publicará el relay
    expect(contractErrors(rows[0].payload)).toEqual([]);
  });

  it('una repetición idempotente no genera un segundo evento', async () => {
    const key = randomUUID();
    const first = await createPayment(key);
    const replay = await createPayment(key);

    expect(replay.headers['idempotent-replayed']).toBe('true');
    expect(await outboxOf(first.body.id)).toHaveLength(1);
  });

  it('UNKNOWN → APPROVED por el resolver: 2 eventos en orden, cada uno con su eventId', async () => {
    gateway.next('error');
    const { body } = await createPayment();
    gateway.record(body.id, 'APPROVED');

    await app.get(UnknownPaymentsResolver).runOnce();

    const rows = await outboxOf(body.id);
    expect(rows.map((r) => r.event_type)).toEqual(['payment.unknown', 'payment.approved']);
    expect(rows[0].id).not.toBe(rows[1].id);
    expect(rows[1].payload.data.gatewayRef).toMatch(/^ch_/);
    expect(Date.parse(rows[1].payload.occurredAt)).toBeGreaterThan(Date.parse(rows[0].payload.occurredAt));
  });

  it('si falla el INSERT en el outbox, se revierte también el cambio de estado y su auditoría', async () => {
    const id = await processing();
    // Fallo real de Postgres después del UPDATE, dentro de la misma transacción
    await pool.query(`
      CREATE FUNCTION outbox_fail() RETURNS trigger LANGUAGE plpgsql AS
        $$ BEGIN RAISE EXCEPTION 'outbox caído'; END $$;
      CREATE TRIGGER outbox_fail BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION outbox_fail();
    `);

    try {
      await expect(
        repo.transition(id, PaymentStatus.PROCESSING, PaymentStatus.APPROVED, { gatewayRef: 'ch_x' }),
      ).rejects.toThrow('outbox caído');
    } finally {
      await pool.query('DROP TRIGGER outbox_fail ON outbox; DROP FUNCTION outbox_fail()');
    }

    const payment = await repo.findById(id, 'm_outbox');
    expect(payment).toMatchObject({ status: 'PROCESSING', gatewayRef: null });
    const { rows } = await pool.query(
      `SELECT to_status FROM payment_events WHERE payment_id = $1 ORDER BY id`,
      [id],
    );
    expect(rows.map((r) => r.to_status)).toEqual(['PROCESSING']);
    expect(await outboxOf(id)).toHaveLength(0);
  });

  it('dos transiciones finales concurrentes: solo la ganadora deja evento', async () => {
    const id = await processing();

    const results = await Promise.all([
      repo.transition(id, PaymentStatus.PROCESSING, PaymentStatus.APPROVED),
      repo.transition(id, PaymentStatus.PROCESSING, PaymentStatus.DECLINED),
    ]);
    const winner = results.find(Boolean)!;

    const rows = await outboxOf(id);
    expect(results.filter(Boolean)).toHaveLength(1);
    expect(rows).toHaveLength(1);
    expect(rows[0].payload.data.status).toBe(winner.status);
  });
});
