import { BeforeApplicationShutdown, Controller, Get, Inject, ServiceUnavailableException } from '@nestjs/common';
import { Pool } from 'pg';
import { PG_POOL } from '../database/database.module.js';


@Controller('health')
export class HealthController implements BeforeApplicationShutdown {
    private shuttingDown = false;

    constructor(@Inject(PG_POOL) private readonly pool: Pool) {}

    // Aca tenemos nuestro Liveness que nos informa si el proceso responde.
    @Get('live')
    live() {
        return { status: 'ok' };
    }

    // Mi Readiness que revisa si puede seguir recibiendo trafico, falla durante el apagado o si la bd no responde
    @Get('ready')
    async ready() {
        if( this.shuttingDown ) throw new ServiceUnavailableException('Apagando');
        await this.pool.query('SELECT 1');
        return { status: 'ready' };
    }

    beforeApplicationShutdown() {
        this.shuttingDown = true;
    }

}