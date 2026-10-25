import type { IHeaders, Producer } from "kafkajs";
import { NonRetryableError } from "./deliver.js";

export const BACKOFF_MS = [5_000, 30_000, 120_000];

export function isRetryable(err: unknown) {
    return !( err instanceof NonRetryableError);
}

export async function sendToRetryOrDlt(
    producer: Producer,
    msg: { key: Buffer | null; value: Buffer | null; headers?: IHeaders },
    error: Error,
    retryable: boolean
) {
    const attempt = Number(msg.headers?.attempt?.toString() ?? 0) + 1;
    const goToDlt = !retryable || attempt > BACKOFF_MS.length; // Aca se hara como cantidad de reintentos la longitud del arreglo de milisegundos

    await producer.send({
        topic: goToDlt ? 'payments.events.dlt' : 'payments.events.retry',
        messages: [{
            key: msg.key,
            value: msg.value,
            headers: {
                ...msg.headers,
                attempt: String(attempt),
                'not-before': String(Date.now() + ( BACKOFF_MS[attempt -1] ?? 0 )),
                'last-error': error.message.slice( 0, 500 ),
                'original-topic': 'payments.events'
            }
        }],
    });

    return goToDlt ? 'dlt' : 'retry';
}