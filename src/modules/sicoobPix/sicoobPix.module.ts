import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { SicoobPixController } from './sicoobPix.controller';
import { SicoobPixService } from './sicoobPix.service';
import { SicoobPixTokenService } from './sicoobPix.token.service';

@Module({
  imports: [HttpModule],
  controllers: [SicoobPixController],
  providers: [SicoobPixService, SicoobPixTokenService],
  exports: [SicoobPixService],
})
export class SicoobPixModule {}
