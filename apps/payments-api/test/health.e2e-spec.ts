import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Pool } from 'pg';
import request from 'supertest';
import { inject } from 'vitest';
import { AppModule } from '../src/app.module.js';
import { HealthController } from '../src/health/health.controller.js';
import { createTestApp } from './support/app.js';
import { FakeGateway } from './support/fake-gateway.js';

describe('Health y operación (R9)', () => {
  const gateway = new FakeGateway();
  let app: INestApplication;
  let pool: Pool;

  beforeAll(async () => {
    await gateway.start();
    ({ app, pool } = await createTestApp(gateway));
  });

  afterAll(async () => {
    await app?.close();
    await gateway.stop();
  });

  it('GET /health/live responde 200', async () => {
    await request(app.getHttpServer())
      .get('/health/live')
      .expect(200, { status: 'ok' });
  });

  it('GET /health/ready responde 200 con la BD disponible', async () => {
    await request(app.getHttpServer())
      .get('/health/ready')
      .expect(200, { status: 'ready' });
  });

  it('usa la base de datos de Testcontainers, no la de desarrollo', () => {
    expect(
      (pool as unknown as { options: { connectionString: string } }).options
        .connectionString,
    ).toBe(inject('databaseUrl'));
  });

  it('sirve el contrato OpenAPI en /docs', async () => {
    await request(app.getHttpServer()).get('/docs-json').expect(200);
  });

  it('al apagarse deja de estar listo (503) y cierra el pool', async () => {
    app.get(HealthController).beforeApplicationShutdown();
    const res = await request(app.getHttpServer())
      .get('/health/ready')
      .expect(503);
    expect(res.headers['content-type']).toContain('application/problem+json');
    expect(res.body).toMatchObject({ status: 503, detail: 'Error interno' }); // el filtro oculta el detalle de los 5xx;

    await app.close();
    expect(pool.ended).toBe(true);
  });
});

describe('Arranque sin base de datos', () => {
  it('falla al iniciar (fail fast) si no puede conectar', async () => {
    const previous = process.env.DATABASE_URL;
    process.env.DATABASE_URL = 'postgres://nadie:nada@127.0.0.1:1/ninguna';
    const moduleRef = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    const app = moduleRef.createNestApplication({ logger: false });
    try {
      await expect(app.init()).rejects.toThrow();
    } finally {
      process.env.DATABASE_URL = previous;
      await app.close().catch(() => undefined);
    }
  });
});
