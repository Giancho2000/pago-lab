import { Body, Controller, Get, HttpCode, HttpStatus, Param, ParseUUIDPipe, Post } from '@nestjs/common';
import { MerchantId } from '../common/merchant-id.decorator.js';
import { CreatePaymentDto } from './dto/create-payment.dto.js';
import { PaymentsService } from './payments.service.js';

@Controller('payments')
export class PaymentsController {
  constructor(private readonly payments: PaymentsService) {}

  @Post()
  @HttpCode(HttpStatus.CREATED)
  create(@MerchantId() merchantId: string, @Body() dto: CreatePaymentDto) {
    return this.payments.create(merchantId, dto);
  }

  @Get(':id')
  get(@MerchantId() merchantId: string, @Param('id', ParseUUIDPipe) id: string) {
    return this.payments.getOrFail(id, merchantId);
  }
}