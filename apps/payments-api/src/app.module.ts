import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { DatabaseModule } from './database/database.module.js';
import { HealthController } from './health/health.controller.js';

@Module({
  imports: [ ConfigModule.forRoot({ isGlobal: true }), DatabaseModule ],
  controllers: [ HealthController ],
})
export class AppModule {}
