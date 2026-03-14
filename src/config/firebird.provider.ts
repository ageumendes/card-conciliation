import { Logger, Provider } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as Firebird from 'node-firebird';

export const FIREBIRD_POOL = Symbol('FIREBIRD_POOL');

export const firebirdProvider: Provider = {
  provide: FIREBIRD_POOL,
  inject: [ConfigService],
  useFactory: (configService: ConfigService) => {
    const poolSize = Number(configService.get('FB_POOL_SIZE') ?? 10);
    const options = {
      host: configService.get<string>('FB_HOST') ?? '127.0.0.1',
      port: Number(configService.get('FB_PORT') ?? 3050),
      database: configService.get<string>('FB_DATABASE'),
      user: configService.get<string>('FB_USER'),
      password: configService.get<string>('FB_PASSWORD'),
    };

    const logger = new Logger('FirebirdProvider');
    logger.log(
      `Firebird connect host=${options.host} port=${options.port} db=${options.database}`,
    );

    return Firebird.pool(poolSize, options);
  },
};
