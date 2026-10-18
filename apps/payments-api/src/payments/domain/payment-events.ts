import { randomUUID } from 'node:crypto';
import { PaymentStatus } from './payment-status.js';
import { Payment } from './payment.js';

const EVENT_TYPE: Partial<Record<PaymentStatus, string>> = {
    [PaymentStatus.APPROVED]: 'payment.approved',
    [PaymentStatus.DECLINED]: 'payment.declined',
    [PaymentStatus.FAILED]: 'payment.failed',
    [PaymentStatus.UNKNOWN]: 'payment.unknown',
};

export interface PaymentEvent {
    eventId: string;
    type: string;
    version: 1;
    occurredAt: string;
    data: Pick<Payment, 'id' | 'merchantId' | 'amountCents' | 'currency' | 'status' | 'gatewayRef'>
};

export function buildPaymentEvent(payment: Payment): PaymentEvent | null {
    const type = EVENT_TYPE[payment.status];

    if(!type) return null;
    
    return {
        eventId: randomUUID(),
        type,
        version: 1,
        occurredAt: payment.updatedAt,
        data: {
            id: payment.id,
            merchantId: payment.merchantId,
            amountCents: payment.amountCents,
            currency: payment.currency,
            status: payment.status,
            gatewayRef: payment.gatewayRef
        }
    };
};
