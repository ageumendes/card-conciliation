import { Body, Controller, Get, Post, UseGuards } from '@nestjs/common';
import { AdminGuard } from '../../auth/admin.guard';
import { RemoteEdiImportOptions, RemoteEdiPullOptions, RemoteEdiService } from './remote-edi.service';

@Controller('admin/remote-edi')
@UseGuards(AdminGuard)
export class RemoteEdiController {
  constructor(private readonly remoteEdiService: RemoteEdiService) {}

  @Get('ping')
  ping() {
    return { ok: true, data: this.remoteEdiService.getPingInfo() };
  }

  @Post('pull')
  async pull(@Body() body: RemoteEdiPullOptions = {}) {
    const data = await this.remoteEdiService.pull(body);
    return { ok: true, data };
  }

  @Post('import')
  async importLocal(@Body() body: RemoteEdiImportOptions = {}) {
    const data = await this.remoteEdiService.importLocal(body);
    return { ok: true, data };
  }
}
