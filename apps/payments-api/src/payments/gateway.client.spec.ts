import { ConfigService } from '@nestjs/config';
import { PaymentStatus } from './domain/payment-status.js';
import { GatewayClient } from './gateway.client.js';

describe('GatewayClient', () => {
  const env: Record<string, string> = { GATEWAY_URL: 'http://gw', GATEWAY_TIMEOUT_MS: '100' };
  const config = { getOrThrow: (k: string) => env[k] } as unknown as ConfigService;
  const client = new GatewayClient(config);
  const input = { reference: 'ref', amountCents: 100, currency: 'COP' };

  const mockFetch = (status: number, body: unknown) =>
    vitest.spyOn(global, 'fetch').mockResolvedValue(new Response(JSON.stringify(body), { status }));

  afterEach(() => vitest.restoreAllMocks());

  it('201 → APPROVED', async () => {
    mockFetch(201, { id: 'ch_1' });
    expect(await client.charge(input)).toEqual({ outcome: PaymentStatus.APPROVED, gatewayRef: 'ch_1' });
  });

  it('402 → DECLINED', async () => {
    mockFetch(402, { id: 'ch_2', reason: 'fondos insuficientes' });
    expect((await client.charge(input)).outcome).toBe(PaymentStatus.DECLINED);
  });

  it('503 → UNKNOWN', async () => {
    mockFetch(503, {});
    expect((await client.charge(input)).outcome).toBe(PaymentStatus.UNKNOWN);
  });

  it('400 → FAILED', async () => {
    mockFetch(400, {});
    expect((await client.charge(input)).outcome).toBe(PaymentStatus.FAILED);
  });

  it('timeout → UNKNOWN', async () => {
    vitest.spyOn(global, 'fetch').mockRejectedValue(Object.assign(new Error('t'), { name: 'TimeoutError' }));
    expect(await client.charge(input)).toEqual({ outcome: PaymentStatus.UNKNOWN, reason: 'TimeoutError' });
  });

  it('getCharge 404 → no encontrado', async () => {
    mockFetch(404, {});
    expect(await client.getCharge('ref')).toEqual({ found: false });
  });

  it('getCharge 200 → encontrado', async () => {
    mockFetch(200, { id: 'ch_3', status: 'APPROVED' });
    expect(await client.getCharge('ref')).toEqual({ found: true, status: 'APPROVED', gatewayRef: 'ch_3' });
  });
});