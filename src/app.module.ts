import * as dotenv from 'dotenv';
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { DbModule } from './db/db.service';
import { HealthModule } from './health/health.module';
import { JobsModule } from './modules/jobs/jobs.module';
import { ReconciliationModule } from './modules/reconciliation/reconciliation.module';
import { SipagModule } from './modules/sipag/sipag.module';
import { SicoobPixModule } from './modules/sicoobPix/sicoobPix.module';
import { CieloF360Module } from './integrations/cielo-f360/cielo-f360.module';
import { CieloSftpEdiModule } from './integrations/cielo-sftp-edi/cielo-sftp-edi.module';
import { InterdataImportModule } from './integrations/interdata-import/interdata-import.module';
import { AcquirerImportModule } from './integrations/acquirer-import/acquirer-import.module';
import { LocalAcquirerCsvModule } from './integrations/local-acquirer-csv/local-acquirer-csv.module';
import { RemoteEdiModule } from './integrations/remote-edi/remote-edi.module';
import { MaintenanceModule } from './modules/maintenance/maintenance.module';
import { AuthModule } from './auth/auth.module';

dotenv.config();

const cieloF360Enabled = String(process.env.CIELO_F360_ENABLED ?? 'false') === 'true';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: ['.env', 'src/.env'],
    }),
    ScheduleModule.forRoot(),
    DbModule,
    AuthModule,
    HealthModule,
    ...(cieloF360Enabled ? [CieloF360Module] : []),
    CieloSftpEdiModule,
    InterdataImportModule,
    AcquirerImportModule,
    LocalAcquirerCsvModule,
    RemoteEdiModule,
    SipagModule,
    SicoobPixModule,
    ReconciliationModule,
    MaintenanceModule,
    JobsModule,
  ],
})
export class AppModule {}
