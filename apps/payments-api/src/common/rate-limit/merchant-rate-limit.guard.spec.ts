import { ExecutionContext, HttpException, HttpStatus } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import type { Pool } from 'pg';
import { MerchantRateLimitGuard } from './merchant-rate-limit.guard.js';
import { MerchantRateLimiter, type RateLimitDecision } from './merchant-rate-limiter.js';

const configOf = (env: Record<string, string | undefined>) =>
  ({ get: (key: string) => env[key] }) as unknown as ConfigService;

describe('MerchantRateLimitGuard', () => {
  const build = (opts: { enabled?: string; merchantId?: string | null } = {}) => {
    const consume = vitest.fn<(merchantId: string) => Promise<RateLimitDecision>>();
    const limiter = { consume } as unknown as MerchantRateLimiter;
    const guard = new MerchantRateLimitGuard(limiter, configOf({ RATE_LIMIT_ENABLED: opts.enabled }));

    const headers: Record<string, string> = {};
    const req = { header: () => (opts.merchantId === null ? undefined : (opts.merchantId ?? 'm_1')) };
    const res = { setHeader: (k: string, v: string) => (headers[k] = v) };
    const ctx = {
      switchToHttp: () => ({ getRequest: () => req, getResponse: () => res }),
    } as unknown as ExecutionContext;

    return { guard, consume, headers, ctx };
  };

  afterEach(() => vitest.restoreAllMocks());

  it('permitido → deja pasar e informa el límite y lo que queda', async () => {
    const { guard, consume, headers, ctx } = build();
    consume.mockResolvedValue({ allowed: true, limit: 30, remaining: 29, retryAfterSeconds: 0 });

    await expect(guard.canActivate(ctx)).resolves.toBe(true);
    expect(consume).toHaveBeenCalledWith('m_1');
    expect(headers).toEqual({ 'RateLimit-Limit': '30', 'RateLimit-Remaining': '29' });
  });

  it('agotado → 429 con Retry-After', async () => {
    const { guard, consume, headers, ctx } = build();
    consume.mockResolvedValue({ allowed: false, limit: 30, remaining: 0, retryAfterSeconds: 2 });

    const err = await guard.canActivate(ctx).catch((e) => e);
    expect(err).toBeInstanceOf(HttpException);
    expect(err.getStatus()).toBe(HttpStatus.TOO_MANY_REQUESTS);
    expect(headers).toEqual({ 'RateLimit-Limit': '30', 'RateLimit-Remaining': '0', 'Retry-After': '2' });
  });

  it('sin X-Merchant-Id no consume: el decorador responde 401', async () => {
    const { guard, consume, ctx } = build({ merchantId: null });

    await expect(guard.canActivate(ctx)).resolves.toBe(true);
    expect(consume).not.toHaveBeenCalled();
  });

  it('RATE_LIMIT_ENABLED=false desactiva el límite', async () => {
    const { guard, consume, ctx } = build({ enabled: 'false' });

    await expect(guard.canActivate(ctx)).resolves.toBe(true);
    expect(consume).not.toHaveBeenCalled();
  });

  it('si el limitador falla, deja pasar (fail open) sin cabeceras de límite', async () => {
    const { guard, consume, headers, ctx } = build();
    consume.mockRejectedValue(new Error('connection terminated'));

    await expect(guard.canActivate(ctx)).resolves.toBe(true);
    expect(headers).toEqual({});
  });
});

describe('MerchantRateLimiter · configuración', () => {
  const create = (env: Record<string, string | undefined>) => new MerchantRateLimiter({} as Pool, configOf(env));

  it('usa 30 de ráfaga y 10 por segundo por defecto', () => {
    expect(create({})).toMatchObject({ burst: 30, perSecond: 10 });
  });

  it('acepta valores decimales en la recarga', () => {
    expect(create({ RATE_LIMIT_BURST: '5', RATE_LIMIT_PER_SECOND: '0.5' })).toMatchObject({ burst: 5, perSecond: 0.5 });
  });

  it.each([
    ['RATE_LIMIT_BURST', '0'],
    ['RATE_LIMIT_BURST', '0.5'],
    ['RATE_LIMIT_BURST', 'abc'],
    ['RATE_LIMIT_PER_SECOND', '0'],
    ['RATE_LIMIT_PER_SECOND', '-1'],
  ])('%s=%s inválido → falla al arrancar', (key, value) => {
    expect(() => create({ [key]: value })).toThrow(key);
  });
});
