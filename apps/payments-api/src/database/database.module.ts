import { Global, Inject, Logger, Module, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Pool } from 'pg';

export const PG_POOL = Symbol('PG_POOL');

@Global()
@Module({
  providers: [
    {
      provide: PG_POOL,
      inject: [ConfigService],
      useFactory: (config: ConfigService) =>
        new Pool({
          connectionString: config.getOrThrow<string>('DATABASE_URL'),
          max: 10, // conexiones máximas por réplica
          idleTimeoutMillis: 30_000,

        }),
    },
  ],
  exports: [PG_POOL],
})

export class DatabaseModule implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger('Database');
  constructor(@Inject(PG_POOL) private readonly pool: Pool) {}

   // Verificamos la conexion al arrancar: el pool es lazy, así que forzamos una consulta real
  async onModuleInit() {
    try {
      await this.pool.query('SELECT 1');
      this.logger.log('-------- Base de datos conectada --------');
    } catch (err) {
      this.logger.error(`***** No se pudo conectar a la base de datos: ${(err as Error).message} *****`);
      throw err; // fail fast: sin BD la API no debe arrancar
    }
  }

  // Último paso del graceful shutdown: cerrar las conexiones resolviendo lo que queda pendiente
  async onApplicationShutdown() {
    await this.pool.end();
  }
}