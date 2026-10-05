import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PaymentStatus } from './domain/payment-status.js';


export type ChargeOutcome =
  | PaymentStatus.APPROVED
  | PaymentStatus.DECLINED
  | PaymentStatus.UNKNOWN
  | PaymentStatus.FAILED;

export interface ChargeResult {
  outcome: ChargeOutcome;
  gatewayRef?: string;
  reason?: string;
}

export type ChargeLookup =
  | { found: false }
  | { found: true; status: 'APPROVED' | 'DECLINED'; gatewayRef: string };

@Injectable()
export class GatewayClient {
  private readonly baseUrl: string;
  private readonly timeoutMs: number;

  constructor(config: ConfigService) {
    this.baseUrl = config.getOrThrow<string>('GATEWAY_URL');
    this.timeoutMs = Number(config.getOrThrow<string>('GATEWAY_TIMEOUT_MS'));
  }

  async charge(input: { reference: string; amountCents: number; currency: string }): Promise<ChargeResult> {
    try {
      const res = await fetch(`${this.baseUrl}/charges`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'idempotency-key': input.reference },
        body: JSON.stringify({ amountCents: input.amountCents, currency: input.currency }),
        signal: AbortSignal.timeout(this.timeoutMs),
      });

      if (res.status >= 500) return { outcome: PaymentStatus.UNKNOWN, reason: `pasarela ${res.status}` };

      const data = (await res.json()) as { id?: string; reason?: string };
      if (res.status === 402) return { outcome: PaymentStatus.DECLINED, gatewayRef: data.id, reason: data.reason };
      if (res.ok) return { outcome: PaymentStatus.APPROVED, gatewayRef: data.id };

      // 4xx: la pasarela rechazó la petición; tenemos certeza de que no cobró
      return { outcome: PaymentStatus.FAILED, reason: `pasarela ${res.status}` };
    } catch (err) {
      // Timeout o error de red: NO sabemos si cobró
      return { outcome: PaymentStatus.UNKNOWN, reason: (err as Error).name };
    }
  }

  async getCharge(reference: string): Promise<ChargeLookup> {
    const res = await fetch(`${this.baseUrl}/charges/${reference}`, {
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    if (res.status === 404) return { found: false };
    if (!res.ok) throw new Error(`pasarela ${res.status}`);
    const data = (await res.json()) as { id: string; status: 'APPROVED' | 'DECLINED' };
    return { found: true, status: data.status, gatewayRef: data.id };
  }
}