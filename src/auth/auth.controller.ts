import {
  Body,
  BadRequestException,
  Controller,
  Get,
  Param,
  Post,
  Req,
  Res,
  UseGuards,
} from '@nestjs/common';
import { Request, Response } from 'express';
import { AuthService } from './auth.service';
import { AdminGuard } from './admin.guard';

type CredentialsDto = {
  username: string;
  password: string;
  displayName?: string;
};

type CreateUserDto = {
  username: string;
  password: string;
  displayName?: string;
};

@Controller('auth')
export class AuthController {
  constructor(private readonly authService: AuthService) {}

  @Get('me')
  async me(@Req() req: Request) {
    const rawSessionToken = this.authService.readSessionTokenFromCookie(req.headers.cookie);
    const state = await this.authService.getSessionState(rawSessionToken);
    return {
      ok: true,
      initialized: state.initialized,
      authenticated: state.authenticated,
      user: state.user,
    };
  }

  @Post('setup')
  async setup(
    @Body() body: CredentialsDto,
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    const session = await this.authService.setupFirstUser({
      username: body.username,
      password: body.password,
      displayName: body.displayName,
    });
    res.setHeader('Set-Cookie', this.authService.buildSessionCookie(session.rawToken));
    return {
      ok: true,
      initialized: true,
      authenticated: true,
      user: session.user,
    };
  }

  @Post('login')
  async login(
    @Body() body: CredentialsDto,
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    const session = await this.authService.login({
      username: body.username,
      password: body.password,
      ipAddress: this.extractIp(req),
      userAgent: String(req.headers['user-agent'] ?? ''),
    });
    res.setHeader('Set-Cookie', this.authService.buildSessionCookie(session.rawToken));
    return {
      ok: true,
      initialized: true,
      authenticated: true,
      user: session.user,
    };
  }

  @Post('logout')
  async logout(
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    const rawSessionToken = this.authService.readSessionTokenFromCookie(req.headers.cookie);
    await this.authService.logout(rawSessionToken);
    res.setHeader('Set-Cookie', this.authService.buildClearSessionCookie());
    return { ok: true };
  }

  private extractIp(req: Request) {
    const forwarded = req.headers['x-forwarded-for'];
    if (typeof forwarded === 'string' && forwarded.trim()) {
      return forwarded.split(',')[0].trim();
    }
    if (Array.isArray(forwarded) && forwarded.length) {
      return forwarded[0]?.trim() || null;
    }
    return req.socket?.remoteAddress ?? null;
  }
}

@Controller('admin/auth')
@UseGuards(AdminGuard)
export class AdminAuthController {
  constructor(private readonly authService: AuthService) {}

  @Get('users')
  async listUsers() {
    const users = await this.authService.listUsers();
    return { ok: true, users };
  }

  @Post('users')
  async createUser(@Req() req: Request, @Body() body: CreateUserDto) {
    if (!req.adminUser) {
      throw new BadRequestException('admin_user_context_missing');
    }
    const user = await this.authService.createUser(req.adminUser, body);
    return { ok: true, user };
  }

  @Post('users/:id/approve')
  async approveUser(@Req() req: Request, @Param('id') id: string) {
    if (!req.adminUser) {
      throw new BadRequestException('admin_user_context_missing');
    }
    const userId = Number(id);
    if (!Number.isFinite(userId) || userId <= 0) {
      throw new BadRequestException('user_id_invalido');
    }
    const user = await this.authService.approveUser(req.adminUser, userId);
    return { ok: true, user };
  }
}
