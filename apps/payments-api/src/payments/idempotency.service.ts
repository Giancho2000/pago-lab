import { ConflictException, Inject, Injectable, UnprocessableEntityException } from '@nestjs/common';
import { createHash } from 'node:crypto';
import { Pool } from 'pg';
import { PG_POOL } from '../database/database.module.js';

export interface IdempotentResult<T> {
  statusCode: number;
  body: T;
  replayed: boolean;
}

@Injectable()
export class IdempotencyService {
  constructor(@Inject(PG_POOL) private readonly pool: Pool) {}

  async execute<T>(
    merchantId: string,
    key: string,
    payload: unknown,
    handler: () => Promise<{ statusCode: number; body: T }>,
    recover?: () => Promise<{ statusCode: number; body: T } | null>,
  ): Promise<IdempotentResult<T>> {
    const requestHash = createHash('sha256').update(JSON.stringify(payload)).digest('hex');

    // Reclamo atómico de la clave. Nunca SELECT y luego INSERT: entre ambos hay una carrera.
    const claim = await this.pool.query(
      `INSERT INTO idempotency_keys (merchant_id, key, request_hash)
       VALUES ($1, $2, $3)
       ON CONFLICT (merchant_id, key) DO NOTHING
       RETURNING key`,
      [merchantId, key, requestHash],
    );

    // Aca decidimos que responder dependiendo de la opcion
    if (claim.rowCount === 0) {
      const { rows } = await this.pool.query(
        `SELECT * FROM idempotency_keys WHERE merchant_id = $1 AND key = $2`,
        [merchantId, key],
      );
      const existing = rows[0];
      if (existing.expires_at < new Date()) {
        throw new UnprocessableEntityException('La Idempotency-Key expiró; usa una nueva');
      }
      if (existing.request_hash !== requestHash) {
        throw new UnprocessableEntityException('La Idempotency-Key ya se usó con otro cuerpo');
      }
       if (existing.status === 'IN_PROGRESS') {
        if (existing.locked_until > new Date()) {
          throw new ConflictException('La petición original sigue en proceso; reintenta en unos segundos');
        }
        // El bloqueo venció: el proceso original probablemente murió
        const recovered = recover ? await recover() : null;
        if (recovered) return { ...recovered, replayed: true };

        // No alcanzó a crear el pago: liberamos la clave y volvemos a intentar el reclamo
        await this.pool.query(
          `DELETE FROM idempotency_keys WHERE merchant_id = $1 AND key = $2 AND status = 'IN_PROGRESS'`,
          [merchantId, key],
        );
        return this.execute(merchantId, key, payload, handler, recover);
      }
      return { statusCode: existing.response_code, body: existing.response_body as T, replayed: true };
    }

    // Somos los dueños de la clave: ejecutar y guardar la respuesta
    try {
      const result = await handler();
      await this.pool.query(
        `UPDATE idempotency_keys
            SET status = 'COMPLETED', response_code = $3, response_body = $4
          WHERE merchant_id = $1 AND key = $2`,
        [merchantId, key, result.statusCode, JSON.stringify(result.body)],
      );
      return { ...result, replayed: false };
    } catch (err) {
      // Solo es seguro liberar la clave si el error ocurrió antes de mover dinero.
      // Por eso, en T08, los fallos de la pasarela NO lanzan excepción: quedan como UNKNOWN.
      await this.pool.query(`DELETE FROM idempotency_keys WHERE merchant_id = $1 AND key = $2`, [
        merchantId,
        key,
      ]);
      throw err;
    }
  }
}