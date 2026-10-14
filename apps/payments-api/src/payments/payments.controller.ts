import {
  BadRequestException, Body, Controller, Get, Headers, HttpStatus, Param, ParseUUIDPipe, Post, Query, Res,
} from '@nestjs/common';
import { isUUID } from 'class-validator';
import type { Response } from 'express';
import { MerchantId } from '../common/merchant-id.decorator.js';
import { Payment } from './domain/payment.js';
import { CreatePaymentDto } from './dto/create-payment.dto.js';
import { IdempotencyService } from './idempotency.service.js';
import { PaymentsService } from './payments.service.js';
import { isFinal, PaymentStatus } from './domain/payment-status.js';
import { listPaymentQuery } from './dto/list-payments.query.js';

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

     const result = await this.idempotency.execute<Payment>(
      merchantId,
      key,
      dto,
      async () => {
        const payment = await this.payments.createAndCharge(merchantId, dto, key);
        const statusCode = payment.status === PaymentStatus.UNKNOWN ? HttpStatus.ACCEPTED : HttpStatus.CREATED;
        return { statusCode, body: payment };
      },
      // Solo se usa si la clave quedó huérfana (IN_PROGRESS con el bloqueo vencido)
      async () => {
        const payment = await this.payments.findByIdempotencyKey(merchantId, key);
        if (!payment) return null;
        return { statusCode: isFinal(payment.status) ? HttpStatus.CREATED : HttpStatus.ACCEPTED, body: payment };
      },
    );

    res.status(result.statusCode);
    res.setHeader('Location', `/payments/${result.body.id}`);
    if (result.replayed) res.setHeader('Idempotent-Replayed', 'true');
    return result.body;
  }

  @Get()
  list(@MerchantId() merchantId: string, @Query() query: listPaymentQuery) {
    return this.payments.list(merchantId, query.limit, query.cursor);
  }

  @Get(':id')
  get(@MerchantId() merchantId: string, @Param('id', ParseUUIDPipe) id: string) {
    return this.payments.getOrFail(id, merchantId);
  }
}