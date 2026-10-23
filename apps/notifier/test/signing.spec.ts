import { createHmac } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { signWebhook } from '../src/signing.js'

describe('signWebhook', () => {
    const now = 1_760_900_000_987; //Cantidad de milisegundos fijos

    it('usa segundos (no milisegundos) y produce el formato que exige el receptor', () => {
        expect(signWebhook('{}', 'whsec_local', now)).toMatch(/^t=1760900000,v1=[0-9a-f]{64}$/);
    });

    it('firma "<t>.<cuerpo>" con HMAC-SHA256, igual que verifica merchant-sim', () => {
        const body = '{"eventId":"e1"}';
        const expected = createHmac('sha256', 'whsec_local').update(`1760900000.${body}`).digest('hex');
        expect(signWebhook(body, 'whsec_local', now)).toBe(`t=1760900000,v1=${expected}`);
    });

    it('cambia si cambia un solo byte del cuerpo o el secreto', () => {
        const base = signWebhook('{"a":1}', 'whsec_local', now);

        expect(signWebhook('{"a":2}', 'whsec_local', now)).not.toBe(base);
        expect(signWebhook('{"a":1}', 'otro_secreto', now)).not.toBe(base);
    });
})