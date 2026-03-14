import { Module } from '@nestjs/common';
import { AcquirerImportService } from './acquirer-import.service';
import { AcquirerImportController } from './acquirer-import.controller';
import { ReconciliationModule } from '../../modules/reconciliation/reconciliation.module';

@Module({
  imports: [ReconciliationModule],
  controllers: [AcquirerImportController],
  providers: [AcquirerImportService],
  exports: [AcquirerImportService],
})
export class AcquirerImportModule {}
