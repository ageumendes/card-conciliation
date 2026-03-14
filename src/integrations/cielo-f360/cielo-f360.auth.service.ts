import { HttpService } from '@nestjs/axios';
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { lastValueFrom } from 'rxjs';
import { CieloF360LoginResponse } from './cielo-f360.types';

@Injectable()
export class CieloF360AuthService {
  private readonly logger = new Logger(CieloF360AuthService.name);
  private cachedToken: string | null = null;
  private expiresAt = 0;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {
    const enabled = String(this.configService.get('CIELO_F360_ENABLED') ?? 'false') === 'true';
    const integrationToken = this.configService.get<string>('CIELO_F360_INTEGRATION_TOKEN');
    if (enabled && !integrationToken) {
      throw new Error('CIELO_F360_INTEGRATION_TOKEN obrigatorio');
    }
  }

  async getAccessToken(): Promise<string> {
    if (this.cachedToken && Date.now() < this.expiresAt) {
      return this.cachedToken;
    }
    return this.doLogin();
  }

  async refreshToken(): Promise<string> {
    this.cachedToken = null;
    this.expiresAt = 0;
    return this.doLogin();
  }

  async doLogin(): Promise<string> {
    const enabled = String(this.configService.get('CIELO_F360_ENABLED') ?? 'false') === 'true';
    if (!enabled) {
      throw new Error('CIELO_F360_ENABLED desativado');
    }
    const baseUrl = this.configService.get<string>('CIELO_F360_BASE_URL');
    const integrationToken = this.configService.get<string>('CIELO_F360_INTEGRATION_TOKEN');

    if (!baseUrl || !baseUrl.startsWith('http')) {
      throw new Error('CIELO_F360_BASE_URL invalida');
    }
    if (!integrationToken) {
      throw new Error('CIELO_F360_INTEGRATION_TOKEN obrigatorio');
    }

    this.logger.log(`Autenticando em ${baseUrl}/PublicLoginAPI/DoLogin`);

    const response = await lastValueFrom(
      this.httpService.post<CieloF360LoginResponse>(
        `${baseUrl}/PublicLoginAPI/DoLogin`,
        { token: integrationToken },
        { timeout: 20000 },
      ),
    );

    const token = response.data.Token;
    if (!token) {
      throw new Error('Token JWT nao retornado pela Cielo F360');
    }

    this.cachedToken = token;
    this.expiresAt = Date.now() + 50 * 60 * 1000;
    return token;
  }
}
