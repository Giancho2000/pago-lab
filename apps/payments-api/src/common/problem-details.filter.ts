import { ArgumentsHost, Catch, ExceptionFilter, HttpException, HttpStatus, Logger } from '@nestjs/common';
import { Request, Response } from 'express';

@Catch()
export class ProblemDetailsFilter implements ExceptionFilter {
  private readonly logger = new Logger(ProblemDetailsFilter.name);

  catch(exception: unknown, host: ArgumentsHost) {
    const ctx = host.switchToHttp();
    const res = ctx.getResponse<Response>();
    const req = ctx.getRequest<Request>();

    const isHttp = exception instanceof HttpException;
    const status = isHttp ? exception.getStatus() : HttpStatus.INTERNAL_SERVER_ERROR;
    const payload = isHttp ? exception.getResponse() : null;
    const message =
      typeof payload === 'string' ? payload : (payload as { message?: string | string[] } | null)?.message;

    if (status >= 500) this.logger.error(exception); // el detalle va al log, nunca al cliente

    res
      .status(status)
      .type('application/problem+json')
      .json({
        type: 'about:blank',
        title: HttpStatus[status] ?? 'Error',
        status,
        detail: status >= 500 ? 'Error interno' : Array.isArray(message) ? message.join('; ') : message,
        instance: req.url,
      });
  }
}