import { Module } from '@nestjs/common';
import { CieloSftpEdiService } from './cielo-sftp-edi.service';
import { CieloSftpEdiController } from './cielo-sftp-edi.controller';
import { CieloSftpClient } from './cielo-sftp.client';
import { CieloSftpEdiWatcher } from './cielo-sftp-edi.watcher';
import { ReconciliationModule } from '../../modules/reconciliation/reconciliation.module';

@Module({
  imports: [ReconciliationModule],
  controllers: [CieloSftpEdiController],
  providers: [CieloSftpEdiService, CieloSftpClient, CieloSftpEdiWatcher],
  exports: [CieloSftpEdiService],
})
export class CieloSftpEdiModule {}
