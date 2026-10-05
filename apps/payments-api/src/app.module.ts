import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { DatabaseModule } from './database/database.module.js';
import { HealthController } from './health/health.controller.js';
import { PaymentsModule } from './payments/payments.module.js';

@Module({
  imports: [ ConfigModule.forRoot({ isGlobal: true }), 
    DatabaseModule, 
    PaymentsModule 
  ],
  controllers: [ HealthController ],
})
export class AppModule {}
