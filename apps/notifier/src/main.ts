import { Kafka } from 'kafkajs';
import pg from 'pg';
import pino from 'pino';
import { deliver, type PaymentEvent } from './deliver.js';

const log = pino();
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const kafka = new Kafka({ clientId: 'notifier', brokers: process.env.KAFKA_BROKERS!.split(',') });
const consumer = kafka.consumer({ groupId: 'notifier' });
const cfg = { url: process.env.MERCHANT_WEBHOOK_URL!, secret: process.env.WEBHOOK_SECRET! };

await consumer.connect();
await consumer.subscribe({ topics: ['payments.events'], fromBeginning: true });

await consumer.run({
  autoCommit: false, // confirmamos el offset solo cuando terminamos de procesar
  eachMessage: async ({ topic, partition, message }) => {
    const event = JSON.parse(message.value!.toString()) as PaymentEvent;
    const result = await deliver(pool, event, cfg);
    log.info({ eventId: event.eventId, partition, offset: message.offset, result }, 'evento procesado');

    // El offset confirmado es el SIGUIENTE a leer, por eso +1
    await consumer.commitOffsets([
      { topic, partition, offset: (BigInt(message.offset) + 1n).toString() },
    ]);
  },
});

// Graceful shutdown: salir del grupo limpio acelera el rebalanceo de los demás
for (const signal of ['SIGTERM', 'SIGINT'] as const) {
  process.on(signal, async () => {
    await consumer.disconnect();
    await pool.end();
    process.exit(0);
  });
}