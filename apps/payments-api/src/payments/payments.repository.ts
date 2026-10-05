import { Inject, Injectable } from '@nestjs/common';
import { Pool } from 'pg';
import { PG_POOL } from '../database/database.module.js';
import { withTransaction } from '../common/transaction.js';
import { Payment, PaymentRow, toPayment } from './domain/payment.js';
import { canTransition, PaymentStatus } from './domain/payment-status.js';

export interface NewPayment {
  merchantId: string;
  amountCents: number;
  currency: string;
  description?: string;
}

@Injectable()
export class PaymentsRepository {
  constructor(@Inject(PG_POOL) private readonly pool: Pool) {}

  async insert(data: NewPayment): Promise<Payment> {
    const { rows } = await this.pool.query<PaymentRow>(
      `INSERT INTO payments (merchant_id, amount_cents, currency, description)
       VALUES ($1, $2, $3, $4)
       RETURNING *`,
      [data.merchantId, data.amountCents, data.currency, data.description ?? null],
    );
    return toPayment(rows[0]);
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
      return toPayment(rows[0]);
    });
  }
}