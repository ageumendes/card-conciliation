import { Controller, Get } from '@nestjs/common';
import { DbService } from '../db/db.service';

@Controller()
export class HealthController {
  constructor(private readonly dbService: DbService) {}

  @Get()
  getRoot() {
    return this.getHealth();
  }

  @Get('health')
  getHealth() {
    return {
      ok: true,
      service: 'card-conciliation',
      time: new Date().toISOString(),
    };
  }

  @Get('health/db')
  async getDbHealth() {
    await this.dbService.query('SELECT 1 FROM RDB$DATABASE');
    return { ok: true };
  }
}
