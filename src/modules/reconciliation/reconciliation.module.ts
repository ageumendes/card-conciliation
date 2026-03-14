import { Module } from '@nestjs/common';
import { ReconciliationService } from './reconciliation.service';
import { ReconciliationController } from './reconciliation.controller';
import { ReconciliationRepository } from './reconciliation.repository';
import { LockService } from '../../common/lock/lock.service';
import { ReconciliationMatchRepository } from './reconciliation-match.repository';
import { ReconciliationStatusService } from './reconciliation-status.service';
import { CanonDuplicateCleanupService } from './canon-duplicate-cleanup.service';

@Module({
  providers: [
    ReconciliationService,
    ReconciliationRepository,
    ReconciliationMatchRepository,
    ReconciliationStatusService,
    LockService,
    CanonDuplicateCleanupService,
  ],
  controllers: [ReconciliationController],
  exports: [ReconciliationService, ReconciliationStatusService, CanonDuplicateCleanupService],
})
export class ReconciliationModule {}
