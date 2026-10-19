import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { DatabaseModule } from './database/database.module.js';
import { HealthController } from './health/health.controller.js';
import { PaymentsModule } from './payments/payments.module.js';
import { ScheduleModule } from '@nestjs/schedule';
import { KafkaModule } from './messaging/kafka.module.js';
import { MessagingModule } from './messaging/messaging.module.js';

@Module({
  imports: [ ConfigModule.forRoot({ isGlobal: true }), 
    DatabaseModule, 
    PaymentsModule,
    KafkaModule,
    MessagingModule,
    ScheduleModule.forRoot()
  ],
  controllers: [ HealthController ],
})
export class AppModule {}
