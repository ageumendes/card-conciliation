import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { SipagService } from './sipag.service';
import { SipagController } from './sipag.controller';

@Module({
  imports: [HttpModule],
  controllers: [SipagController],
  providers: [SipagService],
  exports: [SipagService],
})
export class SipagModule {}
