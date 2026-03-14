declare namespace Express {
  namespace Multer {
    interface File {
      fieldname: string;
      originalname: string;
      encoding: string;
      mimetype: string;
      size: number;
      buffer: Buffer;
      destination?: string;
      filename?: string;
      path?: string;
      stream?: NodeJS.ReadableStream;
    }
  }
}

declare module 'express' {
  import type { IncomingMessage, ServerResponse } from 'http';

  export interface Request extends IncomingMessage {
    adminUser?: {
      id: number;
      username: string;
      displayName: string | null;
      isActive: boolean;
      isApproved: boolean;
      isPrimaryAdmin: boolean;
    };
  }
  export interface Response extends ServerResponse {}

  const exp: any;
  export = exp;
}
