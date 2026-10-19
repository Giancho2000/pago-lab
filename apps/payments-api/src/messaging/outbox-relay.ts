import { Inject, Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron, CronExpression, Interval } from '@nestjs/schedule';
import type { Producer } from 'kafkajs';
import { Pool } from 'pg';
import { withTransaction } from '../common/transaction.js';
import { PG_POOL } from '../database/database.module.js';
import { KAFKA_PRODUCER } from './kafka.module.js';

@Injectable()
export class OutboxRelay {
  private readonly logger = new Logger(OutboxRelay.name);
  private running = false;

  constructor(
    @Inject(PG_POOL) private readonly pool: Pool,
    @Inject(KAFKA_PRODUCER) private readonly producer: Producer,
    private readonly config: ConfigService,
  ) {}

  // ---- Publicación ----

  @Interval(1000)
  async tick() {
    if (this.config.get('RELAY_ENABLED') !== 'true' || this.running) return;
    this.running = true;
    try {
      while ((await this.publishBatch()) > 0) { /* vacía el backlog */ }
    } catch (err) {
      this.logger.error(err);
    } finally {
      this.running = false;
    }
  }

  async publishBatch(limit = 100): Promise<number> {
    return withTransaction(this.pool, async (client) => {
      const { rows } = await client.query(
        `SELECT id, aggregate_id, event_type, payload
           FROM outbox
          WHERE published_at IS NULL
          ORDER BY created_at
          LIMIT $1
          FOR UPDATE SKIP LOCKED`,
        [limit],
      );
      if (rows.length === 0) return 0;

      await this.producer.send({
        topic: 'payments.events',
        acks: -1, // all: espera confirmación de las réplicas en sincronía
        messages: rows.map((r) => ({
          key: r.aggregate_id,                 // misma clave = misma partición = orden por pago
          value: JSON.stringify(r.payload),
          headers: { 'event-id': r.id, 'event-type': r.event_type },
        })),
      });

      await client.query(`UPDATE outbox SET published_at = now() WHERE id = ANY($1::uuid[])`, [
        rows.map((r) => r.id),
      ]);
      return rows.length;
    });
  }

  // ---- Limpieza ----

  // Cron y no @Interval(24 h): el contador de @Interval se reinicia en cada despliegue
  @Cron(CronExpression.EVERY_DAY_AT_3AM)
  async purgeTick() {
    if (this.config.get('RELAY_ENABLED') !== 'true') return;
    try {
      const n = await this.purgePublished();
      this.logger.log(`outbox: ${n} eventos publicados purgados`);
    } catch (err) {
      this.logger.error(err);
    }
  }

  // published_at NULL nunca cumple el "<": los pendientes no se borran aunque sean antiguos
  async purgePublished(retentionDays = 7): Promise<number> {
    const { rowCount } = await this.pool.query(
      `DELETE FROM outbox WHERE published_at < now() - make_interval(days => $1)`,
      [retentionDays],
    );
    return rowCount ?? 0;
  }
}
