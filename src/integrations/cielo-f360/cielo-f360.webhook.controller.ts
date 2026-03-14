import { Body, Controller, Logger, Post } from '@nestjs/common';

@Controller('webhooks/cielo-f360')
export class CieloF360WebhookController {
  private readonly logger = new Logger(CieloF360WebhookController.name);

  @Post('relatorio-pronto')
  handleRelatorioPronto(@Body() body: { Service?: string; Value?: { Id?: string; FileName?: string } }) {
    this.logger.log(`Webhook Cielo F360 recebido: ${JSON.stringify(body)}`);
    // TODO: persistir evento de webhook para auditoria.
    return { ok: true };
  }
}
