import { HttpService } from '@nestjs/axios';
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { lastValueFrom } from 'rxjs';

interface SicoobTokenResponse {
  access_token: string;
  expires_in: number;
  token_type?: string;
}

@Injectable()
export class SicoobPixTokenService {
  private readonly logger = new Logger(SicoobPixTokenService.name);
  private cachedToken: string | null = null;
  private expiresAt = 0;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {}

  async getAccessToken(): Promise<string> {
    const now = Date.now();
    if (this.cachedToken && now < this.expiresAt) {
      return this.cachedToken;
    }

    const authUrl = this.configService.get<string>('SICOOB_AUTH_URL');
    const clientId = this.configService.get<string>('SICOOB_CLIENT_ID');
    const scope = this.configService.get<string>('SICOOB_SCOPE');
    const grantType =
      this.configService.get<string>('SICOOB_GRANT_TYPE') ?? 'client_credentials';
    const timeoutMs = Number(this.configService.get('SICOOB_TIMEOUT_MS') ?? 20000);

    if (!authUrl || !authUrl.startsWith('http')) {
      throw new Error('SICOOB_AUTH_URL invalida');
    }

    if (!clientId) {
      throw new Error('SICOOB_CLIENT_ID obrigatorio');
    }

    const params = new URLSearchParams();
    params.set('grant_type', grantType);
    params.set('client_id', clientId);
    if (scope && scope.trim() !== '') {
      params.set('scope', scope);
    }

    this.logger.log(`Solicitando token Sicoob: ${authUrl}`);

    const response = await lastValueFrom(
      this.httpService.post<SicoobTokenResponse>(authUrl, params.toString(), {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        timeout: timeoutMs,
      }),
    );

    const token = response.data.access_token;
    const expiresIn = response.data.expires_in ?? 0;

    if (!token) {
      throw new Error('Resposta de token invalida');
    }

    this.cachedToken = token;
    this.expiresAt = Date.now() + Math.max(expiresIn * 1000 - 60000, 0);

    return token;
  }
}
