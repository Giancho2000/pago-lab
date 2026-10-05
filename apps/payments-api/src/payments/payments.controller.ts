import {
  BadRequestException, Body, Controller, Get, Headers, HttpStatus, Param, ParseUUIDPipe, Post, Res,
} from '@nestjs/common';
import { isUUID } from 'class-validator';
import type { Response } from 'express';
import { MerchantId } from '../common/merchant-id.decorator.js';
import { Payment } from './domain/payment.js';
import { CreatePaymentDto } from './dto/create-payment.dto.js';
import { IdempotencyService } from './idempotency.service.js';
import { PaymentsService } from './payments.service.js';
import { PaymentStatus } from './domain/payment-status.js';

@Controller('payments')
export class PaymentsController {
  constructor(
    private readonly payments: PaymentsService,
    private readonly idempotency: IdempotencyService,
  ) {}

  @Post()
  async create(
    @MerchantId() merchantId: string,
    @Headers('idempotency-key') key: string | undefined,
    @Body() dto: CreatePaymentDto,
    @Res({ passthrough: true }) res: Response,
  ): Promise<Payment> {
    if (!key || !isUUID(key)) {
      throw new BadRequestException('Idempotency-Key es obligatorio y debe ser un UUID');
    }

    const result = await this.idempotency.execute<Payment>(merchantId, key, dto, async () => {
      const payment = await this.payments.createAndCharge(merchantId, dto);
      const statusCode = payment.status === PaymentStatus.UNKNOWN ? HttpStatus.ACCEPTED : HttpStatus.CREATED;
      return { statusCode, body: payment };
    });

    res.status(result.statusCode);
    res.setHeader('Location', `/payments/${result.body.id}`);
    if (result.replayed) res.setHeader('Idempotent-Replayed', 'true');
    return result.body;
  }

  @Get(':id')
  get(@MerchantId() merchantId: string, @Param('id', ParseUUIDPipe) id: string) {
    return this.payments.getOrFail(id, merchantId);
  }
}