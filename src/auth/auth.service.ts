import {
  BadRequestException,
  ConflictException,
  ForbiddenException,
  Injectable,
  Logger,
  OnModuleInit,
  UnauthorizedException,
} from '@nestjs/common';
import { randomBytes, scryptSync, timingSafeEqual, createHash } from 'crypto';
import { DbService } from '../db/db.service';

export type AuthUser = {
  id: number;
  username: string;
  displayName: string | null;
  isActive: boolean;
  isApproved: boolean;
  isPrimaryAdmin: boolean;
};

type AuthSessionRow = {
  ID?: number;
  SESSION_ID?: string;
  USER_ID?: number;
  USERNAME?: string;
  DISPLAY_NAME?: string;
  IS_ACTIVE?: number;
  IS_APPROVED?: number;
  IS_PRIMARY_ADMIN?: number;
  CREATED_BY?: number;
  APPROVED_BY?: number;
  APPROVED_AT?: Date;
  PASSWORD_HASH?: string;
  PASSWORD_SALT?: string;
  EXPIRES_AT?: Date;
  LAST_SEEN_AT?: Date;
};

type AdminUserRow = {
  ID?: number;
  USERNAME?: string;
  DISPLAY_NAME?: string;
  IS_ACTIVE?: number;
  IS_APPROVED?: number;
  IS_PRIMARY_ADMIN?: number;
  CREATED_BY?: number;
  APPROVED_BY?: number;
  APPROVED_AT?: Date;
  CREATED_AT?: Date;
};

@Injectable()
export class AuthService implements OnModuleInit {
  private readonly logger = new Logger(AuthService.name);
  private schemaReadyPromise?: Promise<void>;

  constructor(private readonly dbService: DbService) {}

  async onModuleInit() {
    await this.ensureSchema();
  }

  async ensureSchema() {
    if (!this.schemaReadyPromise) {
      this.schemaReadyPromise = this.ensureSchemaInternal();
    }
    await this.schemaReadyPromise;
  }

  async getSessionState(rawSessionToken?: string | null) {
    await this.ensureSchema();
    const initialized = await this.hasUsers();
    if (!rawSessionToken) {
      return { initialized, authenticated: false as const, user: null };
    }
    const user = await this.findUserBySessionToken(rawSessionToken);
    if (!user) {
      return { initialized, authenticated: false as const, user: null };
    }
    return { initialized, authenticated: true as const, user };
  }

  async setupFirstUser(params: { username: string; password: string; displayName?: string | null }) {
    await this.ensureSchema();
    if (await this.hasUsers()) {
      throw new ConflictException('auth_already_initialized');
    }
    const username = this.normalizeUsername(params.username);
    const password = this.normalizePassword(params.password);
    const displayName = this.normalizeDisplayName(params.displayName, username);
    const salt = randomBytes(16).toString('hex');
    const hash = this.hashPassword(password, salt);

    await this.dbService.execute(
      'INSERT INTO T_ADMIN_USERS (USERNAME, DISPLAY_NAME, PASSWORD_HASH, PASSWORD_SALT, IS_ACTIVE, IS_APPROVED, IS_PRIMARY_ADMIN, CREATED_AT, UPDATED_AT) VALUES (?, ?, ?, ?, 1, 1, 1, ?, ?)',
      [username, displayName, hash, salt, new Date(), new Date()],
    );

    const created = await this.getUserByUsername(username);
    if (created) {
      await this.dbService.execute(
        'UPDATE T_ADMIN_USERS SET APPROVED_BY = ?, APPROVED_AT = ?, UPDATED_AT = ? WHERE ID = ?',
        [created.id, new Date(), new Date(), created.id],
      );
    }

    this.logger.log(`Usuario admin inicial criado: ${username}`);
    return this.createSessionForUser(username);
  }

  async createUser(
    actor: AuthUser,
    params: { username: string; password: string; displayName?: string | null },
  ) {
    await this.ensureSchema();
    const username = this.normalizeUsername(params.username);
    const password = this.normalizePassword(params.password);
    const displayName = this.normalizeDisplayName(params.displayName, username);
    if (await this.getUserByUsername(username)) {
      throw new ConflictException('username_already_exists');
    }
    const salt = randomBytes(16).toString('hex');
    const hash = this.hashPassword(password, salt);
    const now = new Date();
    await this.dbService.execute(
      'INSERT INTO T_ADMIN_USERS (USERNAME, DISPLAY_NAME, PASSWORD_HASH, PASSWORD_SALT, IS_ACTIVE, IS_APPROVED, IS_PRIMARY_ADMIN, CREATED_BY, CREATED_AT, UPDATED_AT) VALUES (?, ?, ?, ?, 1, 0, 0, ?, ?, ?)',
      [username, displayName, hash, salt, actor.id, now, now],
    );
    const created = await this.getUserByUsername(username);
    this.logger.log(`Usuario criado pendente de aprovacao: ${username} por ${actor.username}`);
    return created;
  }

  async listUsers() {
    await this.ensureSchema();
    const rows = await this.dbService.query<AdminUserRow>(
      'SELECT ID, USERNAME, DISPLAY_NAME, IS_ACTIVE, IS_APPROVED, IS_PRIMARY_ADMIN, CREATED_BY, APPROVED_BY, APPROVED_AT, CREATED_AT ' +
        'FROM T_ADMIN_USERS ORDER BY IS_PRIMARY_ADMIN DESC, CREATED_AT ASC, ID ASC',
    );
    return rows.map((row) => this.mapAdminUserRow(row));
  }

  async approveUser(actor: AuthUser, userId: number) {
    await this.ensureSchema();
    if (!actor.isPrimaryAdmin) {
      throw new ForbiddenException('only_primary_admin_can_approve_users');
    }
    const user = await this.getUserById(userId);
    if (!user) {
      throw new BadRequestException('user_not_found');
    }
    if (user.isPrimaryAdmin) {
      throw new BadRequestException('primary_admin_does_not_require_approval');
    }
    if (user.isApproved) {
      return user;
    }
    await this.dbService.execute(
      'UPDATE T_ADMIN_USERS SET IS_APPROVED = 1, APPROVED_BY = ?, APPROVED_AT = ?, UPDATED_AT = ? WHERE ID = ?',
      [actor.id, new Date(), new Date(), userId],
    );
    this.logger.log(`Usuario aprovado: id=${userId} por ${actor.username}`);
    return this.getUserById(userId);
  }

  async login(params: { username: string; password: string; ipAddress?: string | null; userAgent?: string | null }) {
    await this.ensureSchema();
    const username = this.normalizeUsername(params.username);
    const password = this.normalizePassword(params.password);
    const rows = await this.dbService.query<AuthSessionRow>(
      'SELECT FIRST 1 ID, USERNAME, DISPLAY_NAME, PASSWORD_HASH, PASSWORD_SALT, IS_ACTIVE, IS_APPROVED, IS_PRIMARY_ADMIN FROM T_ADMIN_USERS WHERE USERNAME = ?',
      [username],
    );
    const userRow = rows[0];
    if (!userRow || Number(userRow.IS_ACTIVE ?? 0) !== 1) {
      throw new UnauthorizedException('invalid_credentials');
    }
    const expectedHash = String(userRow.PASSWORD_HASH ?? '');
    const salt = String(userRow.PASSWORD_SALT ?? '');
    const actualHash = this.hashPassword(password, salt);
    const expectedBuffer = Buffer.from(expectedHash, 'hex');
    const actualBuffer = Buffer.from(actualHash, 'hex');
    if (
      expectedBuffer.length !== actualBuffer.length ||
      !timingSafeEqual(expectedBuffer, actualBuffer)
    ) {
      throw new UnauthorizedException('invalid_credentials');
    }
    if (Number(userRow.IS_APPROVED ?? 0) !== 1) {
      throw new ForbiddenException('user_pending_primary_admin_approval');
    }

    await this.cleanupExpiredSessions();

    const user = {
      id: Number(userRow.ID),
      username: String(userRow.USERNAME ?? ''),
      displayName: userRow.DISPLAY_NAME ? String(userRow.DISPLAY_NAME) : null,
      isActive: true,
      isApproved: true,
      isPrimaryAdmin: Number(userRow.IS_PRIMARY_ADMIN ?? 0) === 1,
    };
    return this.createSession(user, params.ipAddress, params.userAgent);
  }

  async logout(rawSessionToken?: string | null) {
    await this.ensureSchema();
    if (!rawSessionToken) {
      return;
    }
    await this.dbService.execute(
      'DELETE FROM T_ADMIN_SESSIONS WHERE TOKEN_HASH = ?',
      [this.hashToken(rawSessionToken)],
    );
  }

  async findUserBySessionToken(rawSessionToken?: string | null): Promise<AuthUser | null> {
    await this.ensureSchema();
    if (!rawSessionToken) {
      return null;
    }
    const tokenHash = this.hashToken(rawSessionToken);
    const rows = await this.dbService.query<AuthSessionRow>(
      'SELECT FIRST 1 s.SESSION_ID, s.USER_ID, s.EXPIRES_AT, s.LAST_SEEN_AT, u.USERNAME, u.DISPLAY_NAME, u.IS_ACTIVE, u.IS_APPROVED, u.IS_PRIMARY_ADMIN ' +
        'FROM T_ADMIN_SESSIONS s ' +
        'JOIN T_ADMIN_USERS u ON u.ID = s.USER_ID ' +
        'WHERE s.TOKEN_HASH = ? AND s.IS_ACTIVE = 1',
      [tokenHash],
    );
    const row = rows[0];
    if (!row) {
      return null;
    }
    const expiresAt = row.EXPIRES_AT ? new Date(row.EXPIRES_AT) : null;
    if (
      !expiresAt ||
      expiresAt.getTime() <= Date.now() ||
      Number(row.IS_ACTIVE ?? 0) !== 1 ||
      Number(row.IS_APPROVED ?? 0) !== 1
    ) {
      await this.dbService.execute('DELETE FROM T_ADMIN_SESSIONS WHERE TOKEN_HASH = ?', [tokenHash]);
      return null;
    }

    await this.touchSessionIfNeeded(String(row.SESSION_ID ?? ''), row.LAST_SEEN_AT);

    return {
      id: Number(row.USER_ID),
      username: String(row.USERNAME ?? ''),
      displayName: row.DISPLAY_NAME ? String(row.DISPLAY_NAME) : null,
      isActive: true,
      isApproved: true,
      isPrimaryAdmin: Number(row.IS_PRIMARY_ADMIN ?? 0) === 1,
    };
  }

  getSessionCookieName() {
    return String(process.env.AUTH_COOKIE_NAME ?? 'card_conciliation_session').trim();
  }

  getSessionDurationMs() {
    const days = Number(process.env.AUTH_SESSION_DAYS ?? 7);
    if (!Number.isFinite(days) || days <= 0) {
      return 7 * 24 * 60 * 60 * 1000;
    }
    return Math.trunc(days * 24 * 60 * 60 * 1000);
  }

  buildSessionCookie(rawToken: string) {
    return this.serializeCookie(this.getSessionCookieName(), rawToken, {
      maxAgeSeconds: Math.trunc(this.getSessionDurationMs() / 1000),
    });
  }

  buildClearSessionCookie() {
    return this.serializeCookie(this.getSessionCookieName(), '', { maxAgeSeconds: 0 });
  }

  readSessionTokenFromCookie(cookieHeader?: string | null) {
    if (!cookieHeader) {
      return null;
    }
    const cookies = this.parseCookies(cookieHeader);
    return cookies[this.getSessionCookieName()] ?? null;
  }

  private async createSessionForUser(username: string) {
    const rows = await this.dbService.query<AuthSessionRow>(
      'SELECT FIRST 1 ID, USERNAME, DISPLAY_NAME, IS_ACTIVE, IS_APPROVED, IS_PRIMARY_ADMIN, CREATED_BY, APPROVED_BY, APPROVED_AT, CREATED_AT FROM T_ADMIN_USERS WHERE USERNAME = ?',
      [username],
    );
    const userRow = rows[0];
    if (
      !userRow ||
      Number(userRow.IS_ACTIVE ?? 0) !== 1 ||
      Number(userRow.IS_APPROVED ?? 0) !== 1
    ) {
      throw new ForbiddenException('user_inactive');
    }
    return this.createSession({
      id: Number(userRow.ID),
      username: String(userRow.USERNAME ?? ''),
      displayName: userRow.DISPLAY_NAME ? String(userRow.DISPLAY_NAME) : null,
      isActive: true,
      isApproved: true,
      isPrimaryAdmin: Number(userRow.IS_PRIMARY_ADMIN ?? 0) === 1,
    });
  }

  private async createSession(user: AuthUser, ipAddress?: string | null, userAgent?: string | null) {
    const rawToken = randomBytes(32).toString('hex');
    const sessionId = randomBytes(16).toString('hex');
    const now = new Date();
    const expiresAt = new Date(now.getTime() + this.getSessionDurationMs());
    await this.dbService.execute(
      'INSERT INTO T_ADMIN_SESSIONS (SESSION_ID, USER_ID, TOKEN_HASH, CREATED_AT, UPDATED_AT, LAST_SEEN_AT, EXPIRES_AT, IS_ACTIVE, IP_ADDRESS, USER_AGENT) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)',
      [
        sessionId,
        user.id,
        this.hashToken(rawToken),
        now,
        now,
        now,
        expiresAt,
        ipAddress?.trim() || null,
        userAgent?.trim() || null,
      ],
    );
    return {
      rawToken,
      user,
      expiresAt,
    };
  }

  private async touchSessionIfNeeded(sessionId: string, lastSeenAt?: Date | null) {
    const lastSeen = lastSeenAt ? new Date(lastSeenAt) : null;
    if (lastSeen && Date.now() - lastSeen.getTime() < 5 * 60 * 1000) {
      return;
    }
    await this.dbService.execute(
      'UPDATE T_ADMIN_SESSIONS SET LAST_SEEN_AT = ?, UPDATED_AT = ? WHERE SESSION_ID = ?',
      [new Date(), new Date(), sessionId],
    );
  }

  private async cleanupExpiredSessions() {
    await this.dbService.execute('DELETE FROM T_ADMIN_SESSIONS WHERE EXPIRES_AT <= ?', [new Date()]);
  }

  private async hasUsers() {
    const rows = await this.dbService.query<{ TOTAL?: number }>(
      'SELECT COUNT(*) AS TOTAL FROM T_ADMIN_USERS',
    );
    return Number(rows[0]?.TOTAL ?? 0) > 0;
  }

  private async getUserByUsername(username: string) {
    const rows = await this.dbService.query<AdminUserRow>(
      'SELECT FIRST 1 ID, USERNAME, DISPLAY_NAME, IS_ACTIVE, IS_APPROVED, IS_PRIMARY_ADMIN, CREATED_BY, APPROVED_BY, APPROVED_AT, CREATED_AT FROM T_ADMIN_USERS WHERE USERNAME = ?',
      [username],
    );
    return rows[0] ? this.mapAdminUserRow(rows[0]) : null;
  }

  private async getUserById(id: number) {
    const rows = await this.dbService.query<AdminUserRow>(
      'SELECT FIRST 1 ID, USERNAME, DISPLAY_NAME, IS_ACTIVE, IS_APPROVED, IS_PRIMARY_ADMIN, CREATED_BY, APPROVED_BY, APPROVED_AT, CREATED_AT FROM T_ADMIN_USERS WHERE ID = ?',
      [id],
    );
    return rows[0] ? this.mapAdminUserRow(rows[0]) : null;
  }

  private mapAdminUserRow(row: AdminUserRow) {
    return {
      id: Number(row.ID),
      username: String(row.USERNAME ?? ''),
      displayName: row.DISPLAY_NAME ? String(row.DISPLAY_NAME) : null,
      isActive: Number(row.IS_ACTIVE ?? 0) === 1,
      isApproved: Number(row.IS_APPROVED ?? 0) === 1,
      isPrimaryAdmin: Number(row.IS_PRIMARY_ADMIN ?? 0) === 1,
      createdBy: row.CREATED_BY ? Number(row.CREATED_BY) : null,
      approvedBy: row.APPROVED_BY ? Number(row.APPROVED_BY) : null,
      approvedAt: row.APPROVED_AT ? new Date(row.APPROVED_AT) : null,
      createdAt: row.CREATED_AT ? new Date(row.CREATED_AT) : null,
    };
  }

  private normalizeUsername(value: string) {
    const username = String(value ?? '').trim().toUpperCase();
    if (!username || username.length < 3) {
      throw new BadRequestException('username_invalido');
    }
    return username;
  }

  private normalizePassword(value: string) {
    const password = String(value ?? '');
    if (password.trim().length < 6) {
      throw new BadRequestException('password_invalido');
    }
    return password;
  }

  private normalizeDisplayName(value: string | null | undefined, username: string) {
    const displayName = String(value ?? '').trim();
    return displayName || username;
  }

  private hashPassword(password: string, salt: string) {
    return scryptSync(password, salt, 64).toString('hex');
  }

  private hashToken(token: string) {
    return createHash('sha256').update(token).digest('hex');
  }

  private parseCookies(cookieHeader: string) {
    return cookieHeader.split(';').reduce<Record<string, string>>((acc, part) => {
      const [key, ...rest] = part.split('=');
      const normalizedKey = String(key ?? '').trim();
      if (!normalizedKey) {
        return acc;
      }
      acc[normalizedKey] = decodeURIComponent(rest.join('=').trim());
      return acc;
    }, {});
  }

  private serializeCookie(
    name: string,
    value: string,
    options: { maxAgeSeconds: number },
  ) {
    const parts = [`${name}=${encodeURIComponent(value)}`, 'Path=/', 'HttpOnly', 'SameSite=Lax'];
    parts.push(`Max-Age=${options.maxAgeSeconds}`);
    const secure =
      String(process.env.AUTH_COOKIE_SECURE ?? 'false').trim().toLowerCase() === 'true';
    if (secure) {
      parts.push('Secure');
    }
    return parts.join('; ');
  }

  private async ensureSchemaInternal() {
    await this.ensureTable(
      'T_ADMIN_USERS',
      'CREATE TABLE T_ADMIN_USERS (' +
        'ID BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, ' +
        'USERNAME VARCHAR(60) NOT NULL, ' +
        'DISPLAY_NAME VARCHAR(120), ' +
        'PASSWORD_HASH VARCHAR(255) NOT NULL, ' +
        'PASSWORD_SALT VARCHAR(255) NOT NULL, ' +
        'IS_ACTIVE SMALLINT DEFAULT 1 NOT NULL, ' +
        'IS_APPROVED SMALLINT DEFAULT 0 NOT NULL, ' +
        'IS_PRIMARY_ADMIN SMALLINT DEFAULT 0 NOT NULL, ' +
        'CREATED_BY BIGINT, ' +
        'APPROVED_BY BIGINT, ' +
        'APPROVED_AT TIMESTAMP, ' +
        'CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, ' +
        'UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL' +
      ')',
    );
    await this.ensureTable(
      'T_ADMIN_SESSIONS',
      'CREATE TABLE T_ADMIN_SESSIONS (' +
        'SESSION_ID VARCHAR(64) PRIMARY KEY, ' +
        'USER_ID BIGINT NOT NULL, ' +
        'TOKEN_HASH VARCHAR(128) NOT NULL, ' +
        'CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, ' +
        'UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, ' +
        'LAST_SEEN_AT TIMESTAMP, ' +
        'EXPIRES_AT TIMESTAMP NOT NULL, ' +
        'IS_ACTIVE SMALLINT DEFAULT 1 NOT NULL, ' +
        'IP_ADDRESS VARCHAR(80), ' +
        'USER_AGENT VARCHAR(255)' +
      ')',
    );
    await this.ensureIndex(
      'UX_T_ADMIN_USERS_USERNAME',
      'CREATE UNIQUE INDEX UX_T_ADMIN_USERS_USERNAME ON T_ADMIN_USERS (USERNAME)',
    );
    await this.ensureIndex(
      'UX_T_ADMIN_SESSIONS_TOKEN_HASH',
      'CREATE UNIQUE INDEX UX_T_ADMIN_SESSIONS_TOKEN_HASH ON T_ADMIN_SESSIONS (TOKEN_HASH)',
    );
    await this.ensureIndex(
      'IX_T_ADMIN_SESSIONS_USER_ID',
      'CREATE INDEX IX_T_ADMIN_SESSIONS_USER_ID ON T_ADMIN_SESSIONS (USER_ID)',
    );
    await this.ensureColumn('T_ADMIN_USERS', 'IS_APPROVED', 'ALTER TABLE T_ADMIN_USERS ADD IS_APPROVED SMALLINT DEFAULT 0 NOT NULL');
    await this.ensureColumn('T_ADMIN_USERS', 'IS_PRIMARY_ADMIN', 'ALTER TABLE T_ADMIN_USERS ADD IS_PRIMARY_ADMIN SMALLINT DEFAULT 0 NOT NULL');
    await this.ensureColumn('T_ADMIN_USERS', 'CREATED_BY', 'ALTER TABLE T_ADMIN_USERS ADD CREATED_BY BIGINT');
    await this.ensureColumn('T_ADMIN_USERS', 'APPROVED_BY', 'ALTER TABLE T_ADMIN_USERS ADD APPROVED_BY BIGINT');
    await this.ensureColumn('T_ADMIN_USERS', 'APPROVED_AT', 'ALTER TABLE T_ADMIN_USERS ADD APPROVED_AT TIMESTAMP');
  }

  private async ensureTable(name: string, createSql: string) {
    const rows = await this.dbService.query<{ REL_NAME?: string }>(
      'SELECT FIRST 1 TRIM(r.RDB$RELATION_NAME) AS REL_NAME FROM RDB$RELATIONS r WHERE r.RDB$RELATION_NAME = ?',
      [name],
    );
    if (!rows.length) {
      await this.dbService.execute(createSql);
      this.logger.log(`Tabela criada automaticamente: ${name}`);
    }
  }

  private async ensureIndex(name: string, createSql: string) {
    const rows = await this.dbService.query<{ IDX_NAME?: string }>(
      'SELECT FIRST 1 TRIM(i.RDB$INDEX_NAME) AS IDX_NAME FROM RDB$INDICES i WHERE i.RDB$INDEX_NAME = ?',
      [name],
    );
    if (!rows.length) {
      await this.dbService.execute(createSql);
      this.logger.log(`Indice criado automaticamente: ${name}`);
    }
  }

  private async ensureColumn(relation: string, field: string, alterSql: string) {
    const rows = await this.dbService.query<{ FIELD_NAME?: string }>(
      'SELECT FIRST 1 TRIM(rf.RDB$FIELD_NAME) AS FIELD_NAME FROM RDB$RELATION_FIELDS rf WHERE rf.RDB$RELATION_NAME = ? AND rf.RDB$FIELD_NAME = ?',
      [relation, field],
    );
    if (!rows.length) {
      await this.dbService.execute(alterSql);
      this.logger.log(`Coluna criada automaticamente: ${relation}.${field}`);
    }
  }
}
