import { ConfigService } from '@nestjs/config';
import { GatewayClient } from './gateway.client.js';
import { PaymentsRepository } from './payments.repository.js';
import { UnknownPaymentsResolver } from './unknown-payments.resolver.js';

describe('UnknownPaymentsResolver.tick', () => {
  const build = (enabled: string) => {
    const config = { get: () => enabled } as unknown as ConfigService;
    const resolver = new UnknownPaymentsResolver(
      {} as PaymentsRepository,
      {} as GatewayClient,
      config,
    );
    const runOnce = vitest.spyOn(resolver, 'runOnce');
    return { resolver, runOnce };
  };

  afterEach(() => vitest.restoreAllMocks());

  it('no hace nada si RESOLVER_ENABLED=false', async () => {
    const { resolver, runOnce } = build('false');
    await resolver.tick();
    expect(runOnce).not.toHaveBeenCalled();
  });

  it('no solapa dos ticks de la misma réplica', async () => {
    const { resolver, runOnce } = build('true');
    let finish!: () => void;
    runOnce.mockImplementation(() => new Promise<void>((r) => (finish = r)));

    const first = resolver.tick();
    await resolver.tick(); // llega mientras el primero sigue corriendo
    finish();
    await first;

    expect(runOnce).toHaveBeenCalledTimes(1);
  });

  it('un error no tumba el ciclo y libera el siguiente tick', async () => {
    const { resolver, runOnce } = build('true');
    runOnce
      .mockRejectedValueOnce(new Error('BD caída'))
      .mockResolvedValueOnce();
    const logError = vitest
      .spyOn(resolver['logger'], 'error')
      .mockImplementation(() => undefined);

    await expect(resolver.tick()).resolves.toBeUndefined();
    await resolver.tick();

    expect(logError).toHaveBeenCalledOnce();
    expect(runOnce).toHaveBeenCalledTimes(2);
  });
});
