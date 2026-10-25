import { createHmac } from 'node:crypto';

export function signWebhook(body: string, secret: string, now = Date.now()) {
    const t = Math.floor(now / 1000);
    const v1 = createHmac('sha256',secret).update(`${t}.${body}`).digest('hex');

    return `t=${t},v1=${v1}`;
};