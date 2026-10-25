import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { randomUUID } from 'node:crypto';
import { readdirSync, readFileSync } from 'node:fs';
import pg from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { apply, InvalidEventError, parseEvent, type PaymentEvent } from '../src/apply.js';

// Usamos la migración real, no un CREATE TABLE copiado: si cambia, la prueba se entera
const migrations = new URL('../../../db/migrations/', import.meta.url);
const file = readdirSync(migrations).find((f) => f.endsWith('_create_ledger_schema.sql'))!;
const up = readFileSync(new URL(file, migrations), 'utf8').split('-- migrate:down')[0];

let container: StartedPostgreSqlContainer;
let pool: pg.Pool;

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:17-alpine').start();
  pool = new pg.Pool({ connectionString: container.getConnectionUri() });
  await pool.query(up);
}, 120_000);

afterAll(async () => {
  await pool?.end();
  await container?.stop();
});

beforeEach(() => pool.query('TRUNCATE ledger.processed_events, ledger.merchant_balances'));

const approved = (merchantId = 'm1', amountCents = 1500, currency = 'COP'): PaymentEvent => ({
  eventId: randomUUID(),
  type: 'payment.approved',
  data: { merchantId, amountCents, currency },
});

// bigint llega de pg como texto: lo convertimos en SQL para comparar números
const balances = async () =>
  (await pool.query(
    `SELECT merchant_id, currency, approved_cents::int AS cents, approved_count AS n
       FROM ledger.merchant_balances ORDER BY merchant_id, currency`,
  )).rows;

const processedCount = async () =>
  Number((await pool.query('SELECT count(*) FROM ledger.processed_events')).rows[0].count);

describe('apply (R19)', () => {
  it('aplica un pago aprobado una sola vez aunque el evento llegue repetido', async () => {
    const event = approved();
    expect(await apply(pool, event)).toBe('applied');
    expect(await apply(pool, event)).toBe('duplicate');
    expect(await balances()).toEqual([{ merchant_id: 'm1', currency: 'COP', cents: 1500, n: 1 }]);
  });

  it('el saldo es la suma de los aprobados, separado por comercio y moneda', async () => {
    for (const e of [approved('m1', 1000), approved('m1', 2500), approved('m1', 700, 'USD'), approved('m2', 400)]) {
      await apply(pool, e);
    }
    expect(await balances()).toEqual([
      { merchant_id: 'm1', currency: 'COP', cents: 3500, n: 2 },
      { merchant_id: 'm1', currency: 'USD', cents: 700, n: 1 },
      { merchant_id: 'm2', currency: 'COP', cents: 400, n: 1 },
    ]);
  });

  it('ignora los tipos distintos de payment.approved sin registrarlos', async () => {
    const declined = { ...approved(), type: 'payment.declined' };
    expect(await apply(pool, declined)).toBe('ignored');
    expect(await balances()).toEqual([]);
    expect(await processedCount()).toBe(0);
  });

  it('dos entregas simultáneas del mismo evento: solo una aplica', async () => {
    const event = approved();
    const results = await Promise.all([apply(pool, event), apply(pool, event)]);
    expect(results.sort()).toEqual(['applied', 'duplicate']);
    expect(await balances()).toEqual([{ merchant_id: 'm1', currency: 'COP', cents: 1500, n: 1 }]);
  });

  it('si falla la actualización del saldo, el evento no queda marcado (todo o nada)', async () => {
    const broken = { ...approved(), data: { merchantId: null as unknown as string, amountCents: 1500, currency: 'COP' } };
    await expect(apply(pool, broken)).rejects.toThrow();
    expect(await processedCount()).toBe(0); // la marca se revirtió junto con el saldo
  });
});

describe('parseEvent', () => {
  const buf = (v: unknown) => Buffer.from(typeof v === 'string' ? v : JSON.stringify(v));

  it('rechaza lo que no es JSON y los payment.approved sin monto válido', () => {
    expect(() => parseEvent(buf('esto no es json'))).toThrow(InvalidEventError);
    expect(() => parseEvent(buf({ ...approved(), data: { merchantId: 'm1', amountCents: '1500', currency: 'COP' } }))).toThrow(InvalidEventError);
  });

  it('acepta otros tipos aunque no traigan los datos de saldo (tolerant reader)', () => {
    const declined = { eventId: randomUUID(), type: 'payment.declined' };
    expect(parseEvent(buf(declined)).type).toBe('payment.declined');
  });
}); 