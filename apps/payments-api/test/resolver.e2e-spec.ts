import { INestApplication } from '@nestjs/common';
import { Pool } from 'pg';
import { PaymentStatus } from '../src/payments/domain/payment-status.js';
import { PaymentsRepository } from '../src/payments/payments.repository.js';
import { UnknownPaymentsResolver } from '../src/payments/unknown-payments.resolver.js';
import { createTestApp, truncateAll } from './support/app.js';
import { FakeGateway } from './support/fake-gateway.js';

describe('Resolución de pagos desconocidos (R7)', () => {
  const gateway = new FakeGateway();
  let app: INestApplication;
  let pool: Pool;
  let repo: PaymentsRepository;
  let resolver: UnknownPaymentsResolver;

  // Inserta un pago directamente en la BD, en el estado y la "antigüedad" que necesita cada caso
  const seed = async (
    status: PaymentStatus,
    opts: { ageSeconds?: number; attempts?: number } = {},
  ) => {
    const { rows } = await pool.query<{ id: string }>(
      `INSERT INTO payments (merchant_id, amount_cents, currency, status, attempts, updated_at)
       VALUES ('m_resolver', 1000, 'COP', $1, $2, now() - make_interval(secs => $3))
       RETURNING id`,
      [status, opts.attempts ?? 0, opts.ageSeconds ?? 0],
    );
    return rows[0].id;
  };

  const statusOf = async (id: string) =>
    (
      await pool.query(
        `SELECT status, gateway_ref, attempts, next_check_at FROM payments WHERE id = $1`,
        [id],
      )
    ).rows[0];

  beforeAll(async () => {
    await gateway.start();
    ({ app, pool } = await createTestApp(gateway));
    repo = app.get(PaymentsRepository);
    resolver = app.get(UnknownPaymentsResolver);
  });

  beforeEach(async () => {
    gateway.reset();
    await truncateAll(pool);
  });

  afterEach(() => vitest.restoreAllMocks());

  afterAll(async () => {
    await app?.close();
    await gateway.stop();
  });

  it('UNKNOWN cobrado en la pasarela → APPROVED con gatewayRef', async () => {
    const id = await seed(PaymentStatus.UNKNOWN);
    const charge = gateway.record(id, 'APPROVED');

    await resolver.runOnce();

    expect(await statusOf(id)).toMatchObject({
      status: 'APPROVED',
      gateway_ref: charge.id,
      attempts: 1,
    });
    const { rows } = await pool.query(
      `SELECT from_status, to_status, reason FROM payment_events WHERE payment_id = $1`,
      [id],
    );
    expect(rows).toEqual([
      {
        from_status: 'UNKNOWN',
        to_status: 'APPROVED',
        reason: 'resuelto por consulta a la pasarela',
      },
    ]);
  });

  it('UNKNOWN rechazado en la pasarela → DECLINED', async () => {
    const id = await seed(PaymentStatus.UNKNOWN);
    gateway.record(id, 'DECLINED');
    await resolver.runOnce();
    expect((await statusOf(id)).status).toBe('DECLINED');
  });

  it('la pasarela no lo registra: sigue UNKNOWN hasta agotar los intentos y luego FAILED', async () => {
    const fresh = await seed(PaymentStatus.UNKNOWN);
    const exhausted = await seed(PaymentStatus.UNKNOWN, { attempts: 2 }); // este reclamo es el 3.º

    await resolver.runOnce();

    expect(await statusOf(fresh)).toMatchObject({
      status: 'UNKNOWN',
      attempts: 1,
    });
    expect(await statusOf(exhausted)).toMatchObject({
      status: 'FAILED',
      attempts: 3,
    });
  });

  it('PROCESSING atascado más de 2 minutos pasa a UNKNOWN y se resuelve', async () => {
    const stuck = await seed(PaymentStatus.PROCESSING, { ageSeconds: 180 });
    const recent = await seed(PaymentStatus.PROCESSING, { ageSeconds: 30 });
    gateway.record(stuck, 'APPROVED');
    gateway.record(recent, 'APPROVED');

    await resolver.runOnce();

    expect((await statusOf(stuck)).status).toBe('APPROVED');
    expect((await statusOf(recent)).status).toBe('PROCESSING'); // aún puede estar cobrándose
    const { rows } = await pool.query(
      `SELECT to_status FROM payment_events WHERE payment_id = $1 ORDER BY id`,
      [stuck],
    );
    expect(rows.map((r) => r.to_status)).toEqual(['UNKNOWN', 'APPROVED']);
  });

  it('PROCESSING atascado que otro proceso ya movió se deja en paz', async () => {
    const stuck = await seed(PaymentStatus.PROCESSING, { ageSeconds: 180 });
    vitest.spyOn(repo, 'transition').mockResolvedValueOnce(null);
    const lookup = vitest.spyOn(resolver['gateway'], 'getCharge');

    await resolver.runOnce();

    expect(lookup).not.toHaveBeenCalled();
    expect((await statusOf(stuck)).status).toBe('PROCESSING');
  });

  it('si la consulta a la pasarela falla, el pago queda UNKNOWN para el siguiente ciclo', async () => {
    const id = await seed(PaymentStatus.UNKNOWN);
    gateway.lookupStatus = 500;
    await expect(resolver.runOnce()).resolves.toBeUndefined();
    expect((await statusOf(id)).status).toBe('UNKNOWN');
  });

  it('lease: un pago reclamado no se vuelve a tomar durante 30 segundos', async () => {
    const id = await seed(PaymentStatus.UNKNOWN);

    await resolver.runOnce();
    await resolver.runOnce();

    const row = await statusOf(id);
    expect(row.attempts).toBe(1);
    expect(row.next_check_at.getTime()).toBeGreaterThan(Date.now() + 20_000);
  });

  it('no toma pagos en estado final', async () => {
    await seed(PaymentStatus.APPROVED, { ageSeconds: 600 });
    await seed(PaymentStatus.PENDING, { ageSeconds: 600 });
    expect(await repo.claimForResolution(10)).toHaveLength(0);
  });

  describe('varias réplicas a la vez', () => {
    it('SKIP LOCKED: dos reclamos concurrentes nunca toman el mismo pago', async () => {
      const ids = await Promise.all(
        Array.from({ length: 20 }, () => seed(PaymentStatus.UNKNOWN)),
      );

      const [a, b] = await Promise.all([
        repo.claimForResolution(15),
        repo.claimForResolution(15),
      ]);
      const claimedA = a.map((p) => p.id);
      const claimedB = b.map((p) => p.id);

      expect(claimedA.filter((id) => claimedB.includes(id))).toEqual([]);
      expect(new Set([...claimedA, ...claimedB]).size).toBe(
        Math.min(20, a.length + b.length),
      );
      expect([...claimedA, ...claimedB].every((id) => ids.includes(id))).toBe(
        true,
      );
    });

    it('cada pago se resuelve una sola vez aunque corran dos resolvers', async () => {
      const ids = await Promise.all(
        Array.from({ length: 12 }, () => seed(PaymentStatus.UNKNOWN)),
      );
      ids.forEach((id) => gateway.record(id, 'APPROVED'));

      // Dos instancias comparten la BD como lo harían dos réplicas
      const second = new UnknownPaymentsResolver(
        repo,
        resolver['gateway'],
        resolver['config'],
      );
      await Promise.all([
        resolver.runOnce(),
        second.runOnce(),
        resolver.runOnce(),
        second.runOnce(),
      ]);

      const { rows } = await pool.query<{ payment_id: string; n: number }>(
        `SELECT payment_id, count(*)::int AS n FROM payment_events GROUP BY payment_id`,
      );
      expect(rows).toHaveLength(12);
      expect(rows.every((r) => r.n === 1)).toBe(true);
      const { rows: statuses } = await pool.query(
        `SELECT DISTINCT status FROM payments`,
      );
      expect(statuses).toEqual([{ status: 'APPROVED' }]);
    });
  });
});

describe('PaymentsRepository.transition', () => {
  const gateway = new FakeGateway();
  let app: INestApplication;
  let repo: PaymentsRepository;

  beforeAll(async () => {
    await gateway.start();
    ({ app } = await createTestApp(gateway));
    repo = app.get(PaymentsRepository);
  });

  afterAll(async () => {
    await app?.close();
    await gateway.stop();
  });

  it('rechaza transiciones inválidas sin tocar la BD', async () => {
    const payment = await repo.insert({
      merchantId: 'm_repo',
      amountCents: 100,
      currency: 'COP',
    });
    await expect(
      repo.transition(
        payment.id,
        PaymentStatus.PENDING,
        PaymentStatus.APPROVED,
      ),
    ).rejects.toThrow('Transición inválida: PENDING → APPROVED');
    expect((await repo.findById(payment.id, 'm_repo'))?.status).toBe('PENDING');
  });

  it('es condicional: si el pago ya no está en `from`, devuelve null', async () => {
    const payment = await repo.insert({
      merchantId: 'm_repo',
      amountCents: 100,
      currency: 'COP',
    });
    await repo.transition(
      payment.id,
      PaymentStatus.PENDING,
      PaymentStatus.PROCESSING,
    );
    expect(
      await repo.transition(
        payment.id,
        PaymentStatus.PENDING,
        PaymentStatus.PROCESSING,
      ),
    ).toBeNull();
  });

  it('dos transiciones concurrentes desde el mismo estado: solo una gana', async () => {
    const payment = await repo.insert({
      merchantId: 'm_repo',
      amountCents: 100,
      currency: 'COP',
    });
    const results = await Promise.all([
      repo.transition(
        payment.id,
        PaymentStatus.PENDING,
        PaymentStatus.PROCESSING,
      ),
      repo.transition(
        payment.id,
        PaymentStatus.PENDING,
        PaymentStatus.PROCESSING,
      ),
    ]);
    expect(results.filter(Boolean)).toHaveLength(1);
  });
});
