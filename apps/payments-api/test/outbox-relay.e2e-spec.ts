import { INestApplication } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import request from 'supertest';
import { OutboxRelay } from '../src/messaging/outbox-relay.js';
import { UnknownPaymentsResolver } from '../src/payments/unknown-payments.resolver.js';
import { createTestApp, FakeProducer, truncateAll } from './support/app.js';
import { FakeGateway } from './support/fake-gateway.js';

describe('Outbox relay (R15)', () => {
  const gateway = new FakeGateway();
  let app: INestApplication;
  let pool: Pool;
  let producer: FakeProducer;
  let relay: OutboxRelay;

  const createPayment = () =>
    request(app.getHttpServer())
      .post('/payments')
      .set('X-Merchant-Id', 'm_relay')
      .set('Idempotency-Key', randomUUID())
      .send({ amountCents: 5_000, currency: 'COP' });

  const pending = async () =>
    (await pool.query(`SELECT count(*)::int AS n FROM outbox WHERE published_at IS NULL`)).rows[0].n;

  type SentRecord = {
    topic: string;
    acks: number;
    messages: { key: string; value: string; headers: Record<string, string> }[];
  };
  // Argumento de la llamada n a producer.send
  const sentIn = (call = 0): SentRecord => producer.send.mock.calls[call][0];

  beforeAll(async () => {
    await gateway.start();
    ({ app, pool, producer } = await createTestApp(gateway));
    relay = app.get(OutboxRelay);
  });

  beforeEach(async () => {
    gateway.reset();
    producer.send.mockReset().mockResolvedValue([]);
    await truncateAll(pool);
  });

  afterAll(async () => {
    await app?.close();
    await gateway.stop();
  });

  it('publica con clave = id del pago, cabeceras y en orden de creación; no repite lo publicado', async () => {
    gateway.next('error');
    const { body } = await createPayment(); // payment.unknown
    gateway.record(body.id, 'APPROVED');
    await app.get(UnknownPaymentsResolver).runOnce(); // payment.approved

    const { rows: outbox } = await pool.query(`SELECT id, payload FROM outbox ORDER BY created_at`);

    expect(await relay.publishBatch()).toBe(2);
    expect(await relay.publishBatch()).toBe(0);
    expect(producer.send).toHaveBeenCalledOnce();

    const { topic, acks, messages } = sentIn();
    expect({ topic, acks }).toEqual({ topic: 'payments.events', acks: -1 });
    expect(messages.map((m) => m.headers['event-type'])).toEqual(['payment.unknown', 'payment.approved']);
    messages.forEach((m, i) => {
      expect(m.key).toBe(body.id);
      expect(m.headers['event-id']).toBe(outbox[i].id);
      expect(JSON.parse(m.value)).toEqual(outbox[i].payload);
    });
    expect(await pending()).toBe(0);
  });

  it('si Kafka falla no marca nada, y el siguiente intento reenvía el mismo lote', async () => {
    await createPayment();
    await createPayment();
    producer.send.mockRejectedValueOnce(new Error('broker caído'));

    await expect(relay.publishBatch()).rejects.toThrow('broker caído');
    expect(await pending()).toBe(2);

    expect(await relay.publishBatch()).toBe(2);
    expect(sentIn(1).messages).toEqual(sentIn(0).messages); // at-least-once: mismos event-id
    expect(await pending()).toBe(0);
  });

  it('dos lotes concurrentes no publican el mismo evento (FOR UPDATE SKIP LOCKED)', async () => {
    await createPayment();
    let release!: () => void;
    producer.send.mockImplementationOnce(() => new Promise((r) => (release = () => r([]))));

    const first = relay.publishBatch();
    // Mientras el primero espera el ack, tiene las filas bloqueadas
    await vitest.waitFor(() => expect(producer.send).toHaveBeenCalledOnce());
    expect(await relay.publishBatch()).toBe(0); // las salta: ni espera ni duplica

    release();
    expect(await first).toBe(1);
    expect(producer.send).toHaveBeenCalledOnce();
  });

  it('la limpieza borra solo lo publicado hace más de 7 días, nunca lo pendiente', async () => {
    const insert = async (createdAgo: string, publishedAgo: string | null) =>
      (
        await pool.query(
          `INSERT INTO outbox (id, aggregate_id, event_type, payload, created_at, published_at)
           VALUES ($1, $2, 'payment.approved', '{}', now() - $3::interval, now() - $4::interval)
           RETURNING id`,
          [randomUUID(), randomUUID(), createdAgo, publishedAgo], // now() - NULL = NULL
        )
      ).rows[0].id;

    const old = await insert('10 days', '8 days');
    await insert('3 days', '2 days'); // publicado reciente
    await insert('10 days', null); // pendiente antiguo (Kafka caído mucho tiempo)

    expect(await relay.purgePublished()).toBe(1);
    const ids = (await pool.query(`SELECT id FROM outbox`)).rows.map((r) => r.id);
    expect(ids).toHaveLength(2);
    expect(ids).not.toContain(old);
  });
});
