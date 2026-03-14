import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  Logger,
} from '@nestjs/common';

@Catch()
export class HttpExceptionFilter implements ExceptionFilter {
  private readonly logger = new Logger(HttpExceptionFilter.name);

  catch(exception: unknown, host: ArgumentsHost) {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse();
    const request = ctx.getRequest();
    const method = request?.method;
    const path = request?.url;
    const requestId = request?.id || request?.headers?.['x-request-id'];

    if (exception instanceof HttpException) {
      const status = exception.getStatus();
      const payload = exception.getResponse();
      let message = exception.message;
      if (typeof payload === 'string') {
        message = payload;
      } else if (payload && typeof payload === 'object') {
        const maybe = payload as { message?: string | string[]; error?: string };
        if (Array.isArray(maybe.message)) {
          message = maybe.message[0] ?? message;
        } else if (typeof maybe.message === 'string') {
          message = maybe.message;
        } else if (typeof maybe.error === 'string') {
          message = maybe.error;
        }
      }

      if (status >= 500) {
        this.logger.error(
          `HTTP ${status} ${method ?? ''} ${path ?? ''}`.trim(),
          exception instanceof Error ? exception.stack : undefined,
        );
      }

      response.status(status).json({ ok: false, error: message, details: payload });
      return;
    }

    const error = exception instanceof Error ? exception : new Error('unknown');
    this.logger.error(
      `HTTP 500 ${method ?? ''} ${path ?? ''}`.trim(),
      error.stack,
    );
    const details = {
      method,
      path,
      requestId,
    };
    response.status(500).json({ ok: false, error: 'internal_error', details });
  }
}
