import { Test } from '@nestjs/testing';
import { Pool } from 'pg';
import { inject } from 'vitest';
import { AppModule } from '../../src/app.module.js';
import { configureApp } from '../../src/app.setup.js';
import { PG_POOL } from '../../src/database/database.module.js';
import { FakeGateway } from './fake-gateway.js';

export const GATEWAY_TIMEOUT_MS = 500;

/**
 * Levanta la app completa contra el Postgres de Testcontainers y la pasarela falsa.
 * Las variables de process.env tienen prioridad sobre el .env local, así que nunca
 * se toca la base de datos de desarrollo.
 */
export async function createTestApp(
  gateway: FakeGateway,
  env: Record<string, string> = {},
) {
  Object.assign(process.env, {
    DATABASE_URL: inject('databaseUrl'),
    GATEWAY_URL: gateway.url,
    GATEWAY_TIMEOUT_MS: String(GATEWAY_TIMEOUT_MS),
    RESOLVER_ENABLED: 'false', // el resolver se invoca a mano en las pruebas
    ...env,
  });

  const moduleRef = await Test.createTestingModule({
    imports: [AppModule],
  }).compile();
  const app = moduleRef.createNestApplication({ logger: false });
  configureApp(app);
  await app.init();
  return { app, pool: app.get<Pool>(PG_POOL) };
}

export async function truncateAll(pool: Pool) {
  await pool.query('TRUNCATE payment_events, payments, idempotency_keys');
}
