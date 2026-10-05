import { Module } from '@nestjs/common';
import { PaymentsController } from './payments.controller.js';
import { PaymentsRepository } from './payments.repository.js';
import { PaymentsService } from './payments.service.js';
import { IdempotencyService } from './idempotency.service.js';
import { GatewayClient } from './gateway.client.js';

@Module({
  controllers: [PaymentsController],
  providers: [PaymentsRepository, PaymentsService, IdempotencyService, GatewayClient],
})
export class PaymentsModule {}