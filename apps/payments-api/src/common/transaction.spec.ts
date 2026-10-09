import { Pool } from 'pg';
import { withTransaction } from './transaction.js';

describe('withTransaction', () => {
  const setup = () => {
    const client = {
      query: vitest.fn().mockResolvedValue({}),
      release: vitest.fn(),
    };
    const pool = {
      connect: vitest.fn().mockResolvedValue(client),
    } as unknown as Pool;
    const sql = () => client.query.mock.calls.map((c) => c[0]);
    return { client, pool, sql };
  };

  it('hace COMMIT y devuelve el resultado', async () => {
    const { client, pool, sql } = setup();
    await expect(withTransaction(pool, async () => 42)).resolves.toBe(42);
    expect(sql()).toEqual(['BEGIN', 'COMMIT']);
    expect(client.release).toHaveBeenCalledOnce();
  });

  it('hace ROLLBACK, propaga el error y libera la conexión', async () => {
    const { client, pool, sql } = setup();
    const boom = new Error('boom');
    await expect(
      withTransaction(pool, async () => {
        throw boom;
      }),
    ).rejects.toBe(boom);
    expect(sql()).toEqual(['BEGIN', 'ROLLBACK']);
    expect(client.release).toHaveBeenCalledOnce();
  });
});
