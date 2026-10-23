import { Pool } from 'pg';
import { signWebhook } from './signing.js';


export class NonRetryableError extends Error {}

export interface PaymentEvent {
    eventId: string;
    type: string;
    data: { id:string };
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function parseEvent(value: Buffer | null): PaymentEvent {
    let event: any;
    try {
        event = JSON.parse(value?.toString() ?? '');
    } catch {
        throw new NonRetryableError('el mensaje no es JSON');
    }
    if (!UUID.test(event?.eventId ?? '') || typeof event?.data?.id !== 'string') {
        throw new NonRetryableError('evento sin eventId (uuid) o sin data.id');
    }
    return event;
}


export async function deliver(pool: Pool, event: PaymentEvent, cfg: { url: string, secret: string }) {

    // 1. ¿Ya lo entregamos? (un rebalanceo o un reinicio pueden traerlo de nuevo)
    const { rowCount } = await pool.query(`SELECT 1 FROM notifier.deliveries WHERE event_id = $1`, [event.eventId]);
    if ( rowCount ) return 'duplicate';

    // 2. Enviar. Un efecto HTTP NO puede ser exactly-once: si morimos entre el envío y el INSERT,
    //    se reenviará. Por eso el receptor también deduplica por eventId.
    const body = JSON.stringify(event);
    const res = await fetch(cfg.url, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'pagolab-signature': signWebhook(body, cfg.secret) },
        body,
        signal: AbortSignal.timeout(5000),
    });
    if (res.status === 400 || res.status === 401) throw new NonRetryableError(`rechazo ${res.status}`);
    if (!res.ok) throw new Error(`webhook ${res.status}`);

    // 3. Registrar la entrega
    await pool.query(
        `INSERT INTO notifier.deliveries (event_id, payment_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
        [event.eventId, event.data.id],
    );
    return 'delivered';
}
