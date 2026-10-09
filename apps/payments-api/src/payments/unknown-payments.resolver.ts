import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Interval } from '@nestjs/schedule';
import { Payment } from './domain/payment.js';
import { PaymentStatus } from './domain/payment-status.js';
import { GatewayClient } from './gateway.client.js';
import { PaymentsRepository } from './payments.repository.js';

const MAX_NOT_FOUND_ATTEMPTS = 3;

@Injectable()
export class UnknownPaymentsResolver {
  private readonly logger = new Logger(UnknownPaymentsResolver.name);
  private running = false;

  constructor(
    private readonly repo: PaymentsRepository,
    private readonly gateway: GatewayClient,
    private readonly config: ConfigService,
  ) {}

  @Interval(10_000)
  async tick() {
    if (this.config.get('RESOLVER_ENABLED') === 'false' || this.running) return;
    this.running = true; // evita que dos ticks de la MISMA réplica se solapen
    try {
      await this.runOnce();
    } catch (err) {
      this.logger.error(err);
    } finally {
      this.running = false;
    }
  }

  async runOnce(): Promise<void> {
    const batch = await this.repo.claimForResolution(10);
    for (const payment of batch) await this.resolve(payment);
  }

  private async resolve(payment: Payment): Promise<void> {
    // Un PROCESSING atascado (el proceso murió a mitad del cobro) pasa primero a UNKNOWN
    if (payment.status === PaymentStatus.PROCESSING) {
      const moved = await this.repo.transition(payment.id, PaymentStatus.PROCESSING, PaymentStatus.UNKNOWN, {
        reason: 'procesamiento atascado',
      });
      if (!moved) return;
    }

    try {
      const lookup = await this.gateway.getCharge(payment.id);

      if (!lookup.found) {
        // Solo declaramos FAILED tras varias consultas: la pasarela podría registrar tarde
        if (payment.attempts >= MAX_NOT_FOUND_ATTEMPTS) {
          await this.repo.transition(payment.id, PaymentStatus.UNKNOWN, PaymentStatus.FAILED, {
            reason: 'la pasarela no registra el cobro',
          });
        }
        return;
      }

      const to = lookup.status === 'APPROVED' ? PaymentStatus.APPROVED : PaymentStatus.DECLINED;
      await this.repo.transition(payment.id, PaymentStatus.UNKNOWN, to, {
        gatewayRef: lookup.gatewayRef,
        reason: 'resuelto por consulta a la pasarela',
      });
      this.logger.log(`Pago ${payment.id} resuelto como ${to}`);
    } catch (err) {
      this.logger.warn(`No se pudo consultar ${payment.id}: ${(err as Error).message}`);
    }
  }
}