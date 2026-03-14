import axios, { AxiosError, AxiosInstance, AxiosRequestConfig } from 'axios';
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { CieloF360AuthService } from './cielo-f360.auth.service';

@Injectable()
export class CieloF360Client {
  private readonly logger = new Logger(CieloF360Client.name);
  private readonly http: AxiosInstance;

  constructor(
    private readonly configService: ConfigService,
    private readonly authService: CieloF360AuthService,
  ) {
    const baseURL = this.configService.get<string>('CIELO_F360_BASE_URL');
    if (!baseURL || !baseURL.startsWith('http')) {
      throw new Error('CIELO_F360_BASE_URL invalida');
    }

    this.http = axios.create({
      baseURL,
      timeout: 20000,
    });

    this.http.interceptors.request.use(async (config) => {
      const token = await this.authService.getAccessToken();
      config.headers = config.headers ?? {};
      config.headers.Authorization = `Bearer ${token}`;
      return config;
    });
  }

  async get<T>(url: string, config?: AxiosRequestConfig): Promise<T> {
    return this.requestWithRetry<T>({ ...config, method: 'GET', url });
  }

  async post<T>(url: string, data?: unknown, config?: AxiosRequestConfig): Promise<T> {
    return this.requestWithRetry<T>({ ...config, method: 'POST', url, data });
  }

  private async requestWithRetry<T>(config: AxiosRequestConfig): Promise<T> {
    try {
      const response = await this.http.request<T>(config);
      return response.data;
    } catch (error) {
      const axiosError = error as AxiosError;
      if (axiosError.response?.status === 401) {
        this.logger.warn('JWT expirado, renovando e repetindo request');
        await this.authService.refreshToken();
        const retryResponse = await this.http.request<T>(config);
        return retryResponse.data;
      }
      throw error;
    }
  }
}
