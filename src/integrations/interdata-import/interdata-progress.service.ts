import { Injectable } from '@nestjs/common';
import { EventEmitter } from 'events';

type ProgressPayload = {
  uploadId: string;
  percent: number;
  stage: 'upload' | 'parse' | 'insert' | 'complete' | 'error';
  message?: string;
};

@Injectable()
export class InterdataProgressService {
  private readonly emitter = new EventEmitter();
  private readonly progressById = new Map<string, ProgressPayload>();
  private manualImporting = false;

  start(uploadId: string, message?: string) {
    this.update({
      uploadId,
      percent: 0,
      stage: 'parse',
      message,
    });
  }

  update(payload: ProgressPayload) {
    this.progressById.set(payload.uploadId, payload);
    this.emitter.emit(payload.uploadId, payload);
  }

  complete(uploadId: string) {
    this.update({
      uploadId,
      percent: 100,
      stage: 'complete',
    });
    setTimeout(() => {
      this.progressById.delete(uploadId);
    }, 60_000);
  }

  error(uploadId: string, message?: string) {
    this.update({
      uploadId,
      percent: 100,
      stage: 'error',
      message,
    });
    setTimeout(() => {
      this.progressById.delete(uploadId);
    }, 60_000);
  }

  get(uploadId: string) {
    return this.progressById.get(uploadId);
  }

  hasActiveUploads(): boolean {
    for (const payload of this.progressById.values()) {
      if (payload.stage !== 'complete' && payload.stage !== 'error') {
        return true;
      }
    }
    return false;
  }

  startManualImport() {
    this.manualImporting = true;
  }

  endManualImport() {
    this.manualImporting = false;
  }

  isBusy(): boolean {
    return this.manualImporting || this.hasActiveUploads();
  }

  subscribe(uploadId: string, handler: (payload: ProgressPayload) => void) {
    this.emitter.on(uploadId, handler);
    return () => this.emitter.off(uploadId, handler);
  }
}
