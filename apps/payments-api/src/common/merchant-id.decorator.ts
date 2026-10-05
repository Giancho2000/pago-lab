import { createParamDecorator, ExecutionContext, UnauthorizedException } from '@nestjs/common';
import { Request } from 'express';

// Temporal: Esto es para simular un comercio.
export const MerchantId = createParamDecorator((_: unknown, ctx: ExecutionContext): string => {
  const req = ctx.switchToHttp().getRequest<Request>();
  const merchantId = req.header('x-merchant-id');
  if (!merchantId) throw new UnauthorizedException('Falta el header X-Merchant-Id');
  return merchantId;
});