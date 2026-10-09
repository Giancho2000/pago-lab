import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { readdirSync, readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { Client } from 'pg';
import type { TestProject } from 'vitest/node';

declare module 'vitest' {
  export interface ProvidedContext {
    databaseUrl: string;
  }
}

const MIGRATIONS_DIR = resolve(
  import.meta.dirname,
  '../../../../db/migrations',
);

// Aplica solo la sección "-- migrate:up" de cada migración de dbmate, en orden
async function migrate(databaseUrl: string) {
  const client = new Client({ connectionString: databaseUrl });
  await client.connect();
  try {
    for (const file of readdirSync(MIGRATIONS_DIR)
      .filter((f) => f.endsWith('.sql'))
      .sort()) {
      const sql = readFileSync(join(MIGRATIONS_DIR, file), 'utf8');
      const up = sql.split('-- migrate:down')[0].replace('-- migrate:up', '');
      await client.query(up);
    }
  } finally {
    await client.end();
  }
}

// Un solo Postgres real para toda la corrida e2e; cada archivo de pruebas levanta su propia app
export default async function setup(project: TestProject) {
  const container = await new PostgreSqlContainer('postgres:17-alpine').start();
  const databaseUrl = container.getConnectionUri();
  await migrate(databaseUrl);
  project.provide('databaseUrl', databaseUrl);

  return async () => {
    await container.stop();
  };
}
