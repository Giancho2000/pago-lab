import { PaymentStatus } from './payment-status.js';

export interface Payment {
  id: string;
  merchantId: string;
  amountCents: number;
  currency: string;
  description: string | null;
  status: PaymentStatus;
  gatewayRef: string | null;
  attempts: number;
  createdAt: string;
  updatedAt: string;
}

export interface PaymentRow {
  id: string;
  merchant_id: string;
  amount_cents: string; // pg devuelve bigint como string para tener una buena precision
  currency: string;
  description: string | null;
  status: PaymentStatus;
  gateway_ref: string | null;
  attempts: number;
  created_at: Date;
  updated_at: Date;
}

export function toPayment(row: PaymentRow): Payment {
  return {
    id: row.id,
    merchantId: row.merchant_id,
    amountCents: Number(row.amount_cents), // seguro hasta 2^53; nuestro máximo es 1.000.000.000
    currency: row.currency,
    description: row.description,
    status: row.status,
    gatewayRef: row.gateway_ref,
    attempts: row.attempts,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
  };
}