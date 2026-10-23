import type { Pool } from 'pg';

export class InvalidEventError extends Error {}

export interface PaymentEvent {
  eventId: string;
  type: string;
  data: { merchantId: string; amountCents: number; currency: string };
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// Valida en la frontera: un mensaje inservible se rechaza aquí, antes de tocar la BD
export function parseEvent(value: Buffer | null): PaymentEvent {
  let event: any;
  try {
    event = JSON.parse(value?.toString() ?? '');
  } catch {
    throw new InvalidEventError('el mensaje no es JSON');
  }
  if (!UUID.test(event?.eventId ?? '') || typeof event?.type !== 'string') {
    throw new InvalidEventError('evento sin eventId (uuid) o sin type');
  }
  if (event.type === 'payment.approved') {
    const { merchantId, amountCents, currency } = event.data ?? {};
    if (typeof merchantId !== 'string' || !Number.isInteger(amountCents) || amountCents <= 0 || typeof currency !== 'string') {
      throw new InvalidEventError('payment.approved sin merchantId, amountCents o currency válidos');
    }
  }
  return event;
}

export async function apply(pool: Pool, event: PaymentEvent) {
  if (event.type !== 'payment.approved') return 'ignored';

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // La marca de "procesado" y el efecto viven en la MISMA transacción
    const first = await client.query(
      `INSERT INTO ledger.processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING RETURNING 1`,
      [event.eventId],
    );
    if (first.rowCount === 0) {
      await client.query('ROLLBACK');
      return 'duplicate';
    }

    await client.query(
      `INSERT INTO ledger.merchant_balances (merchant_id, currency, approved_cents, approved_count)
       VALUES ($1, $2, $3, 1)
       ON CONFLICT (merchant_id, currency) DO UPDATE
         SET approved_cents = ledger.merchant_balances.approved_cents + EXCLUDED.approved_cents,
             approved_count = ledger.merchant_balances.approved_count + 1`,
      [event.data.merchantId, event.data.currency, event.data.amountCents],
    );
    await client.query('COMMIT');
    return 'applied';
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {}); // que un fallo del ROLLBACK no tape el error original
    throw err;
  } finally {
    client.release();
  }
}