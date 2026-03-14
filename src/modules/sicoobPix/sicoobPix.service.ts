import { HttpService } from '@nestjs/axios';
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createHash } from 'crypto';
import { lastValueFrom } from 'rxjs';
import { DbService } from '../../db/db.service';
import { mapSicoobPixToNormalized } from './sicoobPix.mapper';
import {
  NormalizedPixTx,
  SicoobPixRaw,
  SicoobPixResponse,
} from './sicoobPix.types';
import { SicoobPixTokenService } from './sicoobPix.token.service';

@Injectable()
export class SicoobPixService {
  private readonly logger = new Logger(SicoobPixService.name);

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly dbService: DbService,
    private readonly tokenService: SicoobPixTokenService,
  ) {}

  async fetchReceivedPix(refDate: string): Promise<SicoobPixResponse[]> {
    const baseUrl = this.configService.get<string>('SICOOB_PIX_BASE_URL');
    const timeoutMs = Number(this.configService.get('SICOOB_TIMEOUT_MS') ?? 20000);

    if (!baseUrl || !baseUrl.startsWith('http')) {
      throw new Error('SICOOB_PIX_BASE_URL invalida');
    }

    const token = await this.tokenService.getAccessToken();
    const inicio = `${refDate}T00:00:00`;
    const fim = `${refDate}T23:59:59`;

    const payloads: SicoobPixResponse[] = [];
    let pagina = 1;

    while (true) {
      const params = { inicio, fim, pagina };
      this.logger.log(
        `Consultando Pix Sicoob: ${baseUrl}/pix?inicio=${inicio}&fim=${fim}&pagina=${pagina}`,
      );

      try {
        const response = await lastValueFrom(
          this.httpService.get<SicoobPixResponse>('/pix', {
            baseURL: baseUrl,
            headers: {
              Authorization: `Bearer ${token}`,
            },
            params,
            timeout: timeoutMs,
          }),
        );

        const payload = response.data;
        payloads.push(payload);

        if (!this.hasNextPage(payload, pagina)) {
          break;
        }

        pagina += 1;
      } catch (error) {
        const status = (error as any)?.response?.status;
        const data = (error as any)?.response?.data;
        this.logger.error(
          `Erro ao consultar Pix Sicoob: status=${status ?? 'n/a'} body=${
            data ? JSON.stringify(data) : 'n/a'
          }`,
        );
        throw error;
      }
    }

    return payloads;
  }

  async importDminus1(
    refDate: string,
  ): Promise<{ imported: number; rawId: number }> {
    const payloads = await this.fetchReceivedPix(refDate);
    const rawId = await this.saveRawPayload(refDate, payloads);
    const items = this.extractTransactions(payloads);
    const normalized = items.map((item) => mapSicoobPixToNormalized(item, refDate));
    await this.saveNormalized(refDate, rawId, normalized);
    return { imported: normalized.length, rawId };
  }

  private extractTransactions(payloads: SicoobPixResponse[]): SicoobPixRaw[] {
    const items: SicoobPixRaw[] = [];

    for (const payload of payloads) {
      if (Array.isArray(payload.pix)) {
        items.push(...payload.pix);
        continue;
      }

      if (Array.isArray(payload.recebimentos)) {
        items.push(...payload.recebimentos);
        continue;
      }

      if (Array.isArray(payload.items)) {
        items.push(...payload.items);
      }
    }

    return items;
  }

  private hasNextPage(payload: SicoobPixResponse, pagina: number): boolean {
    const paginacao = payload.paginacao;
    if (!paginacao) {
      return false;
    }

    if (typeof paginacao.proximaPagina === 'number') {
      return paginacao.proximaPagina > pagina;
    }

    if (typeof paginacao.totalPaginas === 'number') {
      return pagina < paginacao.totalPaginas;
    }

    if (typeof paginacao.hasMore === 'boolean') {
      return paginacao.hasMore;
    }

    return false;
  }

  private async saveRawPayload(
    refDate: string,
    payloads: SicoobPixResponse[],
  ): Promise<number> {
    const rawJson = JSON.stringify(payloads);
    const hash = createHash('sha256').update(rawJson).digest('hex');

    const sql =
      'INSERT INTO PIX_RAW (PROVIDER, REF_DATE, SOURCE_TYPE, PAYLOAD, PAYLOAD_SHA256, IMPORTED_AT) VALUES (?, ?, ?, ?, ?, ?)';

    await this.dbService.execute(sql, [
      'SICOOB',
      refDate,
      'API',
      rawJson,
      hash,
      new Date(),
    ]);

    const rows = await this.dbService.query<{ ID: number }>(
      'SELECT FIRST 1 ID FROM PIX_RAW WHERE PROVIDER = ? AND REF_DATE = ? ORDER BY ID DESC',
      ['SICOOB', refDate],
    );

    return rows[0]?.ID ?? 0;
  }

  private async saveNormalized(
    refDate: string,
    rawId: number,
    items: NormalizedPixTx[],
  ) {
    if (items.length === 0) {
      return;
    }

    const sql =
      'INSERT INTO PIX_TX (PROVIDER, REF_DATE, END_TO_END_ID, TXID, VALOR, CHAVE, DEVOLUCAO_STATUS, STATUS, HORARIO, PAGADOR_CPF_CNPJ, PAGADOR_NOME, RAW_ID, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';

    for (const item of items) {
      await this.dbService.execute(sql, [
        'SICOOB',
        refDate,
        item.endToEndId ?? null,
        item.txid ?? null,
        item.valor ?? null,
        item.chave ?? null,
        null,
        item.status ?? null,
        item.horario ?? null,
        item.pagadorCpfCnpj ?? null,
        item.pagadorNome ?? null,
        rawId || null,
        new Date(),
      ]);
    }
  }
}
