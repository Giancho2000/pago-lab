import { Kafka } from 'kafkajs';
import pino from 'pino';

const DLT = 'payments.events.dlt';
const GROUP = 'dlt-replay';
const RETRY_HEADERS = ['attempt', 'not-before', 'last-error', 'original-topic'];

const log = pino();
const kafka = new Kafka({ clientId: 'dlt-replay', brokers: process.env.KAFKA_BROKERS!.split(',') });
const admin = kafka.admin();
const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
const consumer = kafka.consumer({ groupId: GROUP });

// 1. Foto: hasta dónde llega el DLT ahora y hasta dónde llegó este grupo la última vez
await admin.connect();
const ends = await admin.fetchTopicOffsets(DLT);                          // [{ partition, low, high }]
const [committed] = await admin.fetchOffsets({ groupId: GROUP, topics: [DLT] });
await admin.disconnect();

const pending = new Map<number, bigint>();                                // partición → offset final
for (const { partition, low, high } of ends) {
  const done = committed.partitions.find((p) => p.partition === partition)?.offset ?? '-1';
  const start = BigInt(done === '-1' ? low : done);                       // -1 = el grupo nunca leyó
  if (start < BigInt(high)) pending.set(partition, BigInt(high));
}
if (pending.size === 0) {
  log.info('DLT sin mensajes pendientes');
  process.exit(0);
}

// 2. Republicar hasta alcanzar la foto
let replayed = 0;
let finish!: () => void;
const finished = new Promise<void>((resolve) => (finish = resolve));

await producer.connect();
await consumer.connect();
await consumer.subscribe({ topics: [DLT], fromBeginning: true });
await consumer.run({
  autoCommit: false,
  eachMessage: async ({ topic, partition, message }) => {
    const headers = Object.fromEntries(
      Object.entries(message.headers ?? {}).filter(([name]) => !RETRY_HEADERS.includes(name)),
    );
    await producer.send({ topic: 'payments.events', messages: [{ key: message.key, value: message.value, headers }] });

    const next = BigInt(message.offset) + 1n;
    await consumer.commitOffsets([{ topic, partition, offset: next.toString() }]);
    replayed++;
    if (next >= (pending.get(partition) ?? 0n)) pending.delete(partition);
    if (pending.size === 0) finish();
  },
});

await finished;
await consumer.disconnect();
await producer.disconnect();
log.info({ replayed }, 'DLT reprocesado');