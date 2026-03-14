import { Module } from '@nestjs/common';
import { InterdataImportService } from './interdata-import.service';
import { InterdataImportController } from './interdata-import.controller';
import { InterdataProgressService } from './interdata-progress.service';
import { InterdataImportWatcher } from './interdata-import.watcher';
import { ReconciliationModule } from '../../modules/reconciliation/reconciliation.module';
import { RemoteEdiModule } from '../remote-edi/remote-edi.module';

@Module({
  imports: [ReconciliationModule, RemoteEdiModule],
  controllers: [InterdataImportController],
  providers: [InterdataImportService, InterdataProgressService, InterdataImportWatcher],
  exports: [InterdataImportService, InterdataProgressService],
})
export class InterdataImportModule {}
