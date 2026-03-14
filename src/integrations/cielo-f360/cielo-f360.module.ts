import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { CieloF360Client } from './cielo-f360.client';
import { CieloF360Service } from './cielo-f360.service';
import { CieloF360AuthService } from './cielo-f360.auth.service';
import { CieloF360Controller } from './cielo-f360.controller';
import { CieloF360WebhookController } from './cielo-f360.webhook.controller';

@Module({
  imports: [HttpModule],
  controllers: [CieloF360Controller, CieloF360WebhookController],
  providers: [CieloF360AuthService, CieloF360Client, CieloF360Service],
  exports: [CieloF360Service],
})
export class CieloF360Module {}
