import { Injectable, NotFoundException } from '@nestjs/common';
import { CreatePaymentDto } from './dto/create-payment.dto.js';
import { Payment } from './domain/payment.js';
import { PaymentsRepository } from './payments.repository.js';

@Injectable()
export class PaymentsService {
  constructor(private readonly repo: PaymentsRepository) {}

  create(merchantId: string, dto: CreatePaymentDto): Promise<Payment> {
    return this.repo.insert({ merchantId, ...dto });
  }

  async getOrFail(id: string, merchantId: string): Promise<Payment> {
    const payment = await this.repo.findById(id, merchantId);
    if (!payment) throw new NotFoundException('Pago no encontrado'); // 404 aunque exista para otro comercio
    return payment;
  }
}