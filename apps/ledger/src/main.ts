import { Kafka } from 'kafkajs';
import pg from 'pg';
import pino from 'pino';
import { apply, InvalidEventError, parseEvent } from './apply.js';

const log = pino();
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const kafka = new Kafka({ clientId: 'ledger', brokers: process.env.KAFKA_BROKERS!.split(',') });
const consumer = kafka.consumer({ groupId: 'ledger' });

await consumer.connect();
await consumer.subscribe({ topics: ['payments.events'], fromBeginning: true });

await consumer.run({
  autoCommit: false,
  eachMessage: async ({ topic, partition, message }) => {
    let eventId: string | undefined;
    let result: string;
    try {
      const event = parseEvent(message.value);
      eventId = event.eventId;
      result = await apply(pool, event);
    } catch (err) {
      // Un error de BD se relanza: sin commit, kafkajs reintenta el mismo mensaje
      if (!(err instanceof InvalidEventError)) throw err;
      // Un mensaje inválido nunca será válido: se registra y se salta
      result = 'invalid';
      log.error({ partition, offset: message.offset, error: err.message }, 'evento inválido descartado');
    }
    log.info({ eventId, partition, offset: message.offset, result }, 'evento procesado');

    await consumer.commitOffsets([{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }]);
  },
});

// Graceful shutdown (R21)
for (const signal of ['SIGTERM', 'SIGINT'] as const) {
  process.on(signal, async () => {
    await consumer.disconnect();
    await pool.end();
    process.exit(0);
  });
}