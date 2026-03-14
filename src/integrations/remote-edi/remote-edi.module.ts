import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { CieloSftpEdiModule } from '../cielo-sftp-edi/cielo-sftp-edi.module';
import { LocalAcquirerCsvModule } from '../local-acquirer-csv/local-acquirer-csv.module';
import { RemoteEdiController } from './remote-edi.controller';
import { RemoteEdiService } from './remote-edi.service';

@Module({
  imports: [ConfigModule, CieloSftpEdiModule, LocalAcquirerCsvModule],
  controllers: [RemoteEdiController],
  providers: [RemoteEdiService],
  exports: [RemoteEdiService],
})
export class RemoteEdiModule {}
