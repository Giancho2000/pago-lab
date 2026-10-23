import { Kafka, type KafkaMessage } from 'kafkajs';
import pg from 'pg';
import pino from 'pino';
import { deliver, parseEvent } from './deliver.js';
import { isRetryable, sendToRetryOrDlt } from './retry.js';

const log = pino();
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const kafka = new Kafka({ clientId: 'notifier', brokers: process.env.KAFKA_BROKERS!.split(',') });
const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
const consumer = kafka.consumer({ groupId: 'notifier' });
const retryConsumer = kafka.consumer({ groupId: 'notifier-retry' });
const cfg = { url: process.env.MERCHANT_WEBHOOK_URL!, secret: process.env.WEBHOOK_SECRET! };
let stopping = false;

const nextOffset = (offset: string) => (BigInt(offset) + 1n).toString();

// Entrega un mensaje; si falla, lo deriva a retry o DLT. Solo lanza si Kafka no acepta la derivación.
async function handle(message: KafkaMessage, partition: number) {
  const attempt = message.headers?.attempt?.toString() ?? '0';
  let eventId: string | undefined;
  try {
    const event = parseEvent(message.value);
    eventId = event.eventId;
    const result = await deliver(pool, event, cfg);
    log.info({ eventId, partition, offset: message.offset, attempt, result }, 'evento procesado');
  } catch (err) {
    const result = await sendToRetryOrDlt(producer, message, err as Error, isRetryable(err));
    log.warn({ eventId, partition, offset: message.offset, attempt, result, error: (err as Error).message }, 'entrega fallida');
  }
}

await producer.connect();
await consumer.connect();
await retryConsumer.connect();
await consumer.subscribe({ topics: ['payments.events'], fromBeginning: true });
await retryConsumer.subscribe({ topics: ['payments.events.retry'], fromBeginning: true });

// ---- Consumidor principal: nunca se detiene por un webhook fallido ----
await consumer.run({
  autoCommit: false,
  eachMessage: async ({ topic, partition, message }) => {
    await handle(message, partition);
    // Siempre confirmamos: el mensaje quedó entregado o a salvo en retry/DLT
     await consumer.commitOffsets([{ topic, partition, offset: nextOffset(message.offset) }]);
  },
});

// ---- Consumidor de reintentos: espera hasta not-before sin perder el grupo ----
await retryConsumer.run({
  autoCommit: false,
  eachMessage: async ({ topic, partition, message, heartbeat }) => {
    const notBefore = Number(message.headers?.['not-before']?.toString() ?? 0);
    while (Date.now() < notBefore) {
      if (stopping) return;            // sin commit: al volver se relee este mensaje
      await new Promise((r) => setTimeout(r, 1000));
      await heartbeat();               // sin esto, Kafka cree que el consumidor murió y rebalancea
    }
    await handle(message, partition);
    await retryConsumer.commitOffsets([{ topic, partition, offset: nextOffset(message.offset) }]);
  },
});

// Graceful shutdown (R21): consumidores → productor → pool
for (const signal of ['SIGTERM', 'SIGINT'] as const) {
  process.on(signal, async () => {
    stopping = true;
    await Promise.all([consumer.disconnect(), retryConsumer.disconnect()]);
    await producer.disconnect();
    await pool.end();
    process.exit(0);
  });
}