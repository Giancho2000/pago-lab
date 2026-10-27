import { CanActivate, ExecutionContext, HttpException, HttpStatus, Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import type { Request, Response } from 'express';
import { MerchantRateLimiter } from './merchant-rate-limiter.js';

/**
 * Limita la creación de pagos por comercio (`X-Merchant-Id`).
 *
 * Corre como guard, antes de validar el cuerpo y de reclamar la Idempotency-Key: un `429` no deja
 * rastro, así que el cliente puede reintentar con la misma clave sin riesgo de cobro doble.
 */
@Injectable()
export class MerchantRateLimitGuard implements CanActivate {
  private readonly logger = new Logger(MerchantRateLimitGuard.name);
  private readonly enabled: boolean;

  constructor(
    private readonly limiter: MerchantRateLimiter,
    config: ConfigService,
  ) {
    this.enabled = config.get('RATE_LIMIT_ENABLED') !== 'false';
  }

  async canActivate(ctx: ExecutionContext): Promise<boolean> {
    if (!this.enabled) return true;

    const req = ctx.switchToHttp().getRequest<Request>();
    const res = ctx.switchToHttp().getResponse<Response>();
    const merchantId = req.header('x-merchant-id');
    if (!merchantId) return true; // el decorador @MerchantId responde 401

    let decision;
    try {
      decision = await this.limiter.consume(merchantId);
    } catch (err) {
      // Fail open: el limitador protege, pero no debe ser la causa de que un pago falle.
      // Si la base de datos está caída, el pago fallará igual en el paso siguiente con su propio error.
      this.logger.warn(`Rate limit no disponible, se permite la petición: ${(err as Error).message}`);
      return true;
    }

    res.setHeader('RateLimit-Limit', String(decision.limit));
    res.setHeader('RateLimit-Remaining', String(decision.remaining));
    if (decision.allowed) return true;

    res.setHeader('Retry-After', String(decision.retryAfterSeconds));
    throw new HttpException(
      `Límite de creación de pagos superado para el comercio; reintenta en ${decision.retryAfterSeconds} s`,
      HttpStatus.TOO_MANY_REQUESTS,
    );
  }
}
