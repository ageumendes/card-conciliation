import {
  BadRequestException,
  Controller,
  ForbiddenException,
  Logger,
  Post,
  Query,
  UseGuards,
} from '@nestjs/common';
import { DbService } from '../../db/db.service';
import { AdminGuard } from '../../auth/admin.guard';

@Controller('admin/maintenance')
@UseGuards(AdminGuard)
export class MaintenanceController {
  private readonly logger = new Logger(MaintenanceController.name);

  constructor(private readonly dbService: DbService) {}

  @Post('clear-all')
  async clearAll(@Query('confirm') confirm?: string) {
    this.assertDevOnly();

    if (confirm !== 'YES') {
      throw new BadRequestException('Use confirm=YES para executar a limpeza total');
    }

    const tables = [
      'T_RECONCILIATION_MATCH',
      'T_RECONCILIATION',
      'T_RECON_STATUS',
      'T_APP_LOCKS',
      'T_ADMIN_SESSIONS',
      'T_ADMIN_USERS',
      'RECON_RESULTS',
      'ACQ_TX',
      'ACQ_RAW',
      'PIX_TX',
      'PIX_RAW',
      'T_CIELO_AJUSTES_APPLIED',
      'T_CIELO_SALES',
      'T_EDI_CIELO_AJUSTES_ITENS',
      'T_EDI_CIELO_AJUSTES',
      'T_EDI_FILES',
      'T_IMPORTED_FILES',
      'T_SICREDI_EDI_RECEIVABLE_PAY',
      'T_SICREDI_EDI_RECEIVABLES',
      'T_SICREDI_EDI_FINANCE',
      'T_SICREDI_EDI_SALES',
      'T_SICREDI_EDI_FILES',
      'T_SICREDI_SALES',
      'T_SIPAG_UR_PAYMENTS',
      'T_SIPAG_UR_DEDUCTIONS',
      'T_SIPAG_UR_CONTRACTS',
      'T_SIPAG_UR',
      'T_SIPAG_PAYMENTS',
      'T_SIPAG_SALES',
      'T_INTERDATA_SALES',
      'T_INTERDATA_SALES_DUPLICATE',
      'T_INTERDATA_SALES_INVALID',
      'T_INTERDATA_FILES',
      'JOB_RUNS',
      'SALES',
    ];

    const missingTables: string[] = [];
    for (const table of tables) {
      try {
        await this.dbService.execute(`DELETE FROM ${table}`);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        const normalized = message.toUpperCase();
        if (normalized.includes('TABLE UNKNOWN') || normalized.includes('RELATION UNKNOWN')) {
          missingTables.push(table);
          continue;
        }
        throw error;
      }
    }
    this.logger.warn(
      `Limpeza total executada (DEV): tabelas=${tables.length} faltantes=${missingTables.length}`,
    );
    return {
      ok: true,
      cleared: tables.filter((table) => !missingTables.includes(table)),
      missingTables,
    };
  }

  @Post('clear-sipag')
  async clearSipag(@Query('confirm') confirm?: string) {
    this.assertDevOnly();

    if (confirm !== 'YES') {
      throw new BadRequestException('Use confirm=YES para executar a limpeza SIPAG');
    }

    const tables = [
      'T_RECONCILIATION_MATCH',
      'T_RECONCILIATION',
      'T_RECON_STATUS',
      'T_SIPAG_UR_PAYMENTS',
      'T_SIPAG_UR_DEDUCTIONS',
      'T_SIPAG_UR_CONTRACTS',
      'T_SIPAG_UR',
      'T_SIPAG_PAYMENTS',
      'T_SIPAG_SALES',
      'ACQ_TX',
      'ACQ_RAW',
      'PIX_TX',
      'PIX_RAW',
    ];

    const missingTables: string[] = [];
    for (const table of tables) {
      try {
        await this.dbService.execute(`DELETE FROM ${table}`);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        const normalized = message.toUpperCase();
        if (normalized.includes('TABLE UNKNOWN') || normalized.includes('RELATION UNKNOWN')) {
          missingTables.push(table);
          continue;
        }
        throw error;
      }
    }

    await this.dbService.execute('DELETE FROM T_IMPORTED_FILES WHERE UPPER(ACQUIRER) = ?', ['SIPAG']);
    this.logger.warn(
      `Limpeza SIPAG executada (DEV): tabelas=${tables.length} faltantes=${missingTables.length}`,
    );

    return {
      ok: true,
      cleared: tables.filter((table) => !missingTables.includes(table)),
      missingTables,
      note: 'Removidos tambem hashes SIPAG em T_IMPORTED_FILES',
    };
  }

  private assertDevOnly() {
    const nodeEnv = String(process.env.NODE_ENV ?? '').toLowerCase();
    const appEnv = String(process.env.APP_ENV ?? '').toLowerCase();
    const maintenanceEnabled = String(process.env.MAINTENANCE_ENABLED ?? 'false').toLowerCase() === 'true';
    if (nodeEnv === 'production' || appEnv === 'production') {
      throw new ForbiddenException('Endpoint permitido apenas em ambiente DEV');
    }
    if (!maintenanceEnabled) {
      throw new ForbiddenException('Endpoint desativado. Defina MAINTENANCE_ENABLED=true para usar.');
    }
  }
}
