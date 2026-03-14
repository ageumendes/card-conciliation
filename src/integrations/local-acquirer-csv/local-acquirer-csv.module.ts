import { Module } from '@nestjs/common';
import { AcquirerImportModule } from '../acquirer-import/acquirer-import.module';
import { LocalAcquirerCsvController } from './local-acquirer-csv.controller';
import { LocalAcquirerCsvService } from './local-acquirer-csv.service';
import { LocalAcquirerCsvWatcher } from './local-acquirer-csv.watcher';

@Module({
  imports: [AcquirerImportModule],
  controllers: [LocalAcquirerCsvController],
  providers: [LocalAcquirerCsvService, LocalAcquirerCsvWatcher],
  exports: [LocalAcquirerCsvService],
})
export class LocalAcquirerCsvModule {}
