import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { Pool } from 'pg';

export async function runMigrations(pool: Pool) {
  const dir = join(__dirname, '../../../db/migrations');
  const files = readdirSync(dir).filter((f) => f.endsWith('.sql')).sort();
  for (const file of files) {
    const sql = readFileSync(join(dir, file), 'utf8');
    const up = sql
      .split('-- migrate:down')[0]
      .replace(/-- migrate:up.*\n/, '')
      .replace(/CONCURRENTLY/g, ''); // en pruebas no hace falta y no se permite en bloque transaccional
    await pool.query(up);
  }
}