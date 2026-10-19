import { ConfigService } from '@nestjs/config';
import type { Producer } from 'kafkajs';
import type { Pool } from 'pg';
import { OutboxRelay } from './outbox-relay.js';

describe('OutboxRelay.tick', () => {
  const build = (enabled: string) => {
    const config = { get: () => enabled } as unknown as ConfigService;
    const relay = new OutboxRelay({} as Pool, {} as Producer, config);
    const publishBatch = vitest.spyOn(relay, 'publishBatch');
    return { relay, publishBatch };
  };

  afterEach(() => vitest.restoreAllMocks());

  it('no publica si RELAY_ENABLED no es "true"', async () => {
    const { relay, publishBatch } = build('false');
    await relay.tick();
    expect(publishBatch).not.toHaveBeenCalled();
  });

  it('vacía el backlog en varios lotes hasta que no quedan pendientes', async () => {
    const { relay, publishBatch } = build('true');
    publishBatch.mockResolvedValueOnce(100).mockResolvedValueOnce(37).mockResolvedValueOnce(0);

    await relay.tick();

    expect(publishBatch).toHaveBeenCalledTimes(3);
  });

  it('un error (Kafka caído) se registra y no bloquea el siguiente tick', async () => {
    const { relay, publishBatch } = build('true');
    publishBatch.mockRejectedValueOnce(new Error('broker caído')).mockResolvedValueOnce(0);
    const logError = vitest.spyOn(relay['logger'], 'error').mockImplementation(() => undefined);

    await expect(relay.tick()).resolves.toBeUndefined();
    await relay.tick();

    expect(logError).toHaveBeenCalledOnce();
    expect(publishBatch).toHaveBeenCalledTimes(2);
  });
});
