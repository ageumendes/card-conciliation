import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import SftpClient from 'ssh2-sftp-client';
import { promises as fs } from 'fs';
import * as path from 'path';

@Injectable()
export class CieloSftpClient {
  private readonly logger = new Logger(CieloSftpClient.name);

  constructor(private readonly configService: ConfigService) {}

  async downloadNewFiles(
    localDir: string,
    fileNames: Set<string>,
    glob: string,
  ): Promise<string[]> {
    const host = this.configService.get<string>('CIELO_SFTP_HOST');
    const port = Number(this.configService.get('CIELO_SFTP_PORT') ?? 22);
    const username = this.configService.get<string>('CIELO_SFTP_USER');
    const password = this.configService.get<string>('CIELO_SFTP_PASSWORD');
    const privateKeyPath = this.configService.get<string>('CIELO_SFTP_PRIVATE_KEY_PATH');
    const remoteDir = this.configService.get<string>('CIELO_SFTP_REMOTE_DIR') ?? '/in';
    const strictHostKeyChecking =
      String(this.configService.get('CIELO_SFTP_STRICT_HOSTKEY_CHECKING') ?? 'true') === 'true';

    if (!host || !username) {
      throw new Error('CIELO_SFTP_HOST e CIELO_SFTP_USER sao obrigatorios');
    }

    const sftp = new SftpClient();
    const connectOptions: Record<string, unknown> = {
      host,
      port,
      username,
    };

    if (privateKeyPath) {
      connectOptions.privateKey = await fs.readFile(privateKeyPath, 'utf8');
    } else if (password) {
      connectOptions.password = password;
    }

    if (!strictHostKeyChecking) {
      connectOptions.hostVerifier = () => true;
    }

    this.logger.log(`Conectando SFTP Cielo: ${host}:${port}${remoteDir}`);

    try {
      await sftp.connect(connectOptions);
      const list = await sftp.list(remoteDir);
      const downloaded: string[] = [];

      for (const item of list) {
        if (item.type !== '-') {
          continue;
        }
        if (!this.matchesGlob(item.name, glob)) {
          continue;
        }

        if (fileNames.has(item.name)) {
          continue;
        }

        const remotePath = path.posix.join(remoteDir, item.name);
        const localPath = path.join(localDir, item.name);
        this.logger.log(`Baixando arquivo SFTP: ${remotePath}`);
        await sftp.fastGet(remotePath, localPath);
        downloaded.push(item.name);
      }

      return downloaded;
    } finally {
      await sftp.end();
    }
  }

  private matchesGlob(filename: string, glob: string): boolean {
    if (!glob || glob === '*' || glob === '*.*') {
      return true;
    }
    const escaped = glob.replace(/[-/\\^$+?.()|[\]{}]/g, '\\$&');
    const regex = new RegExp(`^${escaped.replace(/\*/g, '.*').replace(/\?/g, '.')}$`, 'i');
    return regex.test(filename);
  }
}
