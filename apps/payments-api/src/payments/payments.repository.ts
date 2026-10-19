import { Inject, Injectable } from '@nestjs/common';
import { Pool } from 'pg';
import { PG_POOL } from '../database/database.module.js';
import { withTransaction } from '../common/transaction.js';
import { Payment, PaymentRow, toPayment } from './domain/payment.js';
import { canTransition, PaymentStatus } from './domain/payment-status.js';
import { buildPaymentEvent } from './domain/payment-events.js';

export interface NewPayment {
  merchantId: string;
  amountCents: number;
  currency: string;
  description?: string;
  idempotencyKey?: string;
}

@Injectable()
export class PaymentsRepository {
  constructor(@Inject(PG_POOL) private readonly pool: Pool) {}

  async insert(data: NewPayment): Promise<Payment> {
    const { rows } = await this.pool.query<PaymentRow>(
      `INSERT INTO payments (merchant_id, amount_cents, currency, description, idempotency_key)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING *`,
      [data.merchantId, data.amountCents, data.currency, data.description ?? null, data.idempotencyKey ?? null],
    );
    return toPayment(rows[0]);
  }

   async findByIdempotencyKey(merchantId: string, key: string): Promise<Payment | null> {
    const { rows } = await this.pool.query<PaymentRow>(
      `SELECT * FROM payments WHERE merchant_id = $1 AND idempotency_key = $2`,
      [merchantId, key],
    );
    return rows[0] ? toPayment(rows[0]) : null;
  }

  async findById(id: string, merchantId: string): Promise<Payment | null> {
    const { rows } = await this.pool.query<PaymentRow>(
      `SELECT * FROM payments WHERE id = $1 AND merchant_id = $2`,
      [id, merchantId],
    );
    return rows[0] ? toPayment(rows[0]) : null;
  }

  /**
   * Transición condicional: solo cambia si el pago sigue en el estado `from`.
   * Si otro proceso ya lo cambió, el UPDATE afecta 0 filas y devolvemos null.
   */
  async transition(
    id: string,
    from: PaymentStatus,
    to: PaymentStatus,
    opts: { gatewayRef?: string; reason?: string } = {},
  ): Promise<Payment | null> {
    if (!canTransition(from, to)) {
      throw new Error(`Transición inválida: ${from} → ${to}`);
    }

    return withTransaction(this.pool, async (client) => {
      const { rows } = await client.query<PaymentRow>(
        `UPDATE payments
            SET status = $3,
                gateway_ref = COALESCE($4, gateway_ref),
                updated_at = now()
          WHERE id = $1 AND status = $2
          RETURNING *`,
        [id, from, to, opts.gatewayRef ?? null],
      );

      if (rows.length === 0) return null; // otro proceso ganó la carrera
      await client.query(
        `INSERT INTO payment_events (payment_id, from_status, to_status, reason)
         VALUES ($1, $2, $3, $4)`,
        [id, from, to, opts.reason ?? null],
      );

      const updated = toPayment(rows[0]);
      const event = buildPaymentEvent(updated);
      if (event) {
        await client.query(
          `INSERT INTO outbox (id, aggregate_id, event_type, payload) VALUES ($1, $2, $3, $4)`,
          [event.eventId, updated.id, event.type, JSON.stringify(event)],
        );
      }
      return updated;
    });
  }

    /**
   * Reclama un lote para resolver. Transacción cortísima:
   * - FOR UPDATE SKIP LOCKED: cada réplica toma filas distintas sin esperarse.
   * - next_check_at funciona como "lease": nadie más las toma durante 30 segundos.
   * La llamada a la pasarela se hace DESPUÉS, sin bloqueos abiertos.
   */
  async claimForResolution(limit: number): Promise<Payment[]> {
    const { rows } = await this.pool.query<PaymentRow>(
      `UPDATE payments
          SET next_check_at = now() + interval '30 seconds',
              attempts = attempts + 1
        WHERE id IN (
          SELECT id FROM payments
           WHERE (status = 'UNKNOWN'
                  OR (status = 'PROCESSING' AND updated_at < now() - interval '2 minutes'))
             AND (next_check_at IS NULL OR next_check_at < now())
           ORDER BY updated_at
           LIMIT $1
           FOR UPDATE SKIP LOCKED
        )
        RETURNING *`,
      [limit],
    );
    return rows.map(toPayment);
  }

  async listByMerchant(
    merchantId: string,
    limit: number,
    afterId?: string ,
  ): Promise<Payment[]> {
    // Keyset: "dame los siguientes a este punto" en lugar de "salta N filas"
    const { rows } = afterId
      ? await this.pool.query<PaymentRow>(
          `SELECT * FROM payments
            WHERE merchant_id = $1
              AND (created_at, id) < (SELECT created_at, id FROM payments WHERE id = $2 AND merchant_id = $1)
            ORDER BY created_at DESC, id DESC
            LIMIT $3`,
          [merchantId, afterId, limit],
        )
      : await this.pool.query<PaymentRow>(
          `SELECT * FROM payments
            WHERE merchant_id = $1
            ORDER BY created_at DESC, id DESC
            LIMIT $2`,
          [merchantId, limit],
        );
    return rows.map(toPayment);
  }
}