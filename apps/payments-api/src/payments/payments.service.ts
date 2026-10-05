import { Injectable, NotFoundException } from '@nestjs/common';
import { CreatePaymentDto } from './dto/create-payment.dto.js';
import { Payment } from './domain/payment.js';
import { PaymentStatus } from './domain/payment-status.js';
import { GatewayClient } from './gateway.client.js';
import { PaymentsRepository } from './payments.repository.js';

@Injectable()
export class PaymentsService {
  constructor(
    private readonly repo: PaymentsRepository,
    private readonly gateway: GatewayClient,
  ) {}

  async createAndCharge(merchantId: string, dto: CreatePaymentDto): Promise<Payment> {
    const payment = await this.repo.insert({ merchantId, ...dto });

    // Reclamo el pago: solo un proceso puede pasarlo a PROCESSING
    const claimed = await this.repo.transition(payment.id, PaymentStatus.PENDING, PaymentStatus.PROCESSING, {
      reason: 'inicio de cobro',
    });
    if (!claimed) return this.getOrFail(payment.id, merchantId);

    // Nuestro id de pago viaja como clave de idempotencia hacia la pasarela
    const result = await this.gateway.charge({
      reference: payment.id,
      amountCents: payment.amountCents,
      currency: payment.currency,
    });

    const final = await this.repo.transition(payment.id, PaymentStatus.PROCESSING, result.outcome, {
      gatewayRef: result.gatewayRef,
      reason: result.reason,
    });
    return final ?? this.getOrFail(payment.id, merchantId);
  }

  async getOrFail(id: string, merchantId: string): Promise<Payment> {
    const payment = await this.repo.findById(id, merchantId);
    if (!payment) throw new NotFoundException('Pago no encontrado');
    return payment;
  }
}