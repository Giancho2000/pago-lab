import { Inject, Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Pool } from 'pg';
import { PG_POOL } from '../../database/database.module.js';

export interface RateLimitDecision {
  allowed: boolean;
  limit: number;
  remaining: number;
  retryAfterSeconds: number; // 0 si se permitió
}

/**
 * Token bucket por comercio guardado en PostgreSQL.
 *
 * - `RATE_LIMIT_BURST`: capacidad del balde (ráfaga máxima). Un doble clic o un lote corto no se castigan.
 * - `RATE_LIMIT_PER_SECOND`: tokens que se recargan por segundo (ritmo sostenido).
 *
 * La recarga y el consumo ocurren en UNA sola sentencia: `ON CONFLICT DO UPDATE` bloquea la fila del
 * comercio y reevalúa el WHERE sobre la versión más reciente, así que dos réplicas que consumen a la vez
 * nunca gastan el mismo token. Nunca SELECT y luego UPDATE: entre ambos hay una carrera.
 */
@Injectable()
export class MerchantRateLimiter {
  readonly burst: number;
  readonly perSecond: number;

  constructor(
    @Inject(PG_POOL) private readonly pool: Pool,
    config: ConfigService,
  ) {
    // Capacidad >= 1: con menos, el balde nunca juntaría un token y no se podría crear ningún pago
    this.burst = readNumber(config, 'RATE_LIMIT_BURST', 30, (n) => n >= 1, 'un número >= 1');
    this.perSecond = readNumber(config, 'RATE_LIMIT_PER_SECOND', 10, (n) => n > 0, 'un número > 0');
  }

  async consume(merchantId: string): Promise<RateLimitDecision> {
    // available = tokens guardados + lo recargado desde refilled_at, sin pasar de la capacidad.
    // GREATEST(0, …) protege de relojes de transacciones concurrentes que empezaron antes de la última recarga.
    const { rows } = await this.pool.query<{ tokens: number }>(
      `INSERT INTO rate_limit_buckets AS b (merchant_id, tokens, refilled_at)
       VALUES ($1, $2::float8 - 1, now())
       ON CONFLICT (merchant_id) DO UPDATE
          SET tokens = LEAST($2::float8, b.tokens + GREATEST(0, EXTRACT(EPOCH FROM now() - b.refilled_at)::float8) * $3::float8) - 1,
              refilled_at = GREATEST(b.refilled_at, now())
        WHERE LEAST($2::float8, b.tokens + GREATEST(0, EXTRACT(EPOCH FROM now() - b.refilled_at)::float8) * $3::float8) >= 1
       RETURNING tokens`,
      [merchantId, this.burst, this.perSecond],
    );

    if (rows.length) {
      return { allowed: true, limit: this.burst, remaining: Math.floor(rows[0].tokens), retryAfterSeconds: 0 };
    }

    // Rechazado: el balde no se modificó. Calculamos cuánto falta para el próximo token.
    const { rows: [bucket] } = await this.pool.query<{ available: number }>(
      `SELECT LEAST($2::float8, tokens + GREATEST(0, EXTRACT(EPOCH FROM now() - refilled_at)::float8) * $3::float8) AS available
         FROM rate_limit_buckets WHERE merchant_id = $1`,
      [merchantId, this.burst, this.perSecond],
    );
    const missing = 1 - (bucket?.available ?? 0);
    return {
      allowed: false,
      limit: this.burst,
      remaining: 0,
      retryAfterSeconds: Math.max(1, Math.ceil(missing / this.perSecond)),
    };
  }
}

function readNumber(
  config: ConfigService, key: string, fallback: number, isValid: (n: number) => boolean, rule: string,
): number {
  const raw = config.get<string>(key);
  const value = raw === undefined || raw === '' ? fallback : Number(raw);
  if (!Number.isFinite(value) || !isValid(value)) {
    throw new Error(`${key} debe ser ${rule} (recibido: ${raw})`); // fail fast al arrancar
  }
  return value;
}
