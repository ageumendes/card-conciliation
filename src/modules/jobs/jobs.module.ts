import { Module } from '@nestjs/common';
import { Dminus1Job } from './dminus1.job';
import { SipagModule } from '../sipag/sipag.module';
import { SicoobPixModule } from '../sicoobPix/sicoobPix.module';
import { SicoobPixJob } from './sicoob-pix.job';
import { ReconciliationModule } from '../reconciliation/reconciliation.module';
import { InterdataImportModule } from '../../integrations/interdata-import/interdata-import.module';
import { ReconciliationJob } from './reconciliation.job';
import { RemoteEdiModule } from '../../integrations/remote-edi/remote-edi.module';

@Module({
  imports: [SipagModule, SicoobPixModule, ReconciliationModule, InterdataImportModule, RemoteEdiModule],
  providers: [Dminus1Job, SicoobPixJob, ReconciliationJob],
})
export class JobsModule {}
