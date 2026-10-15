import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { CreatePaymentDto } from './dto/create-payment.dto.js';
import { Payment } from './domain/payment.js';
import { PaymentStatus } from './domain/payment-status.js';
import { GatewayClient } from './gateway.client.js';
import { PaymentsRepository } from './payments.repository.js';
import { isUUID } from 'class-validator';

@Injectable()
export class PaymentsService {
  constructor(
    private readonly repo: PaymentsRepository,
    private readonly gateway: GatewayClient,
  ) {}

  async createAndCharge(merchantId: string, dto: CreatePaymentDto, idempotencyKey: string): Promise<Payment> {
    const payment = await this.repo.insert({ merchantId, ...dto, idempotencyKey });

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

  findByIdempotencyKey(merchantId: string, key: string): Promise<Payment | null> {
    return this.repo.findByIdempotencyKey(merchantId, key);
  }

  async getOrFail(id: string, merchantId: string): Promise<Payment> {
    const payment = await this.repo.findById(id, merchantId);
    if (!payment) throw new NotFoundException('Pago no encontrado');
    return payment;
  }

  async list(merchantId: string, limit: number, cursor?: string) {
    let afterId: string  | undefined;
    if (cursor) {
        afterId = Buffer.from(cursor, 'base64url').toString('utf8');
        if (!isUUID(afterId)) throw new BadRequestException('Cursor inválido');
      }
    const items = await this.repo.listByMerchant(merchantId, limit, afterId);
    const last = items.at(-1);
    const nextCursor = items.length === limit && last ? Buffer.from(last.id).toString('base64url') : null;
    return { items, nextCursor };
  }

}