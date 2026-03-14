import { CanActivate, ExecutionContext, ForbiddenException, Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { AuthService } from './auth.service';

@Injectable()
export class AdminGuard implements CanActivate {
  constructor(
    private readonly configService: ConfigService,
    private readonly authService: AuthService,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const request = context.switchToHttp().getRequest();
    const sessionToken = this.authService.readSessionTokenFromCookie(request.headers?.cookie);
    const sessionUser = await this.authService.findUserBySessionToken(sessionToken);
    if (sessionUser) {
      request.adminUser = sessionUser;
      return true;
    }

    const token = this.configService.get<string>('ADMIN_TOKEN');
    if (!token) {
      throw new ForbiddenException('admin_unauthorized');
    }
    const header = request.headers?.authorization as string | undefined;
    const xAdminToken = request.headers?.['x-admin-token'] as string | undefined;

    const bearer = header?.startsWith('Bearer ') ? header.slice('Bearer '.length).trim() : null;
    const provided = bearer || xAdminToken || '';

    if (!provided || provided !== token) {
      throw new ForbiddenException('admin_unauthorized');
    }

    return true;
  }
}
