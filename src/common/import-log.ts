import { promises as fs } from 'fs';
import * as path from 'path';

export type ImportLogStatus =
  | 'ok'
  | 'error'
  | 'skipped'
  | 'processed'
  | 'processed_with_duplicates'
  | 'skipped_duplicate_file'
  | 'unknown_format';

export type ImportLogEvent = {
  category: 'erp_import' | 'acquirer_import' | 'remote_pull' | 'remote_import';
  provider: 'INTERDATA' | 'CIELO' | 'SIPAG' | 'SICREDI' | 'ALL';
  operation: string;
  status: ImportLogStatus;
  fileName?: string;
  details?: Record<string, unknown>;
};

const toDayKey = (date: Date): string => {
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
};

export const appendImportLog = async (event: ImportLogEvent): Promise<void> => {
  try {
    const baseDir = process.env.IMPORT_LOG_DIR?.trim() || './data/logs/imports';
    const resolvedBaseDir = path.resolve(process.cwd(), baseDir);
    await fs.mkdir(resolvedBaseDir, { recursive: true });

    const now = new Date();
    const filePath = path.join(resolvedBaseDir, `${toDayKey(now)}.jsonl`);
    const payload = {
      timestamp: now.toISOString(),
      ...event,
    };
    await fs.appendFile(filePath, `${JSON.stringify(payload)}\n`, 'utf8');
  } catch {
    // Logging must never break import/download flows.
  }
};
