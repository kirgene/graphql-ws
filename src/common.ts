import {Binary, SerializedBinary} from './types/Binary';
import {Subject} from 'rxjs';

export const BINARY_CHUNK_SIZE = 100 * 1024; // 100 KB

export function repeatPromise(promise: () => Promise<boolean>): any {
    return promise().then((repeat: boolean) => repeat && repeatPromise(promise));
}

export function findBinaries(object: any): Binary[] {
  const value: Binary[] = [];
  Object.keys(object || {}).forEach((k) => {
    if (object[k] instanceof Binary) {
      value.push(object[k]);
    } else if (object[k] && typeof object[k] === 'object') {
      value.push(...this.findBinaries(object[k]));
    }
  });
  return value;
}

export function deserializeBinaries(
  object: any,
  callback: (value: SerializedBinary) => Binary,
) {
  Object.keys(object).forEach((k) => {
    if (Binary.isBinary(object[k])) {
      object[k] = callback(object[k]);
    } else if (object[k] && typeof object[k] === 'object') {
      this.deserializeBinaries(object[k], callback);
    }
  });
}

export interface IncomingFile {
  binary: Binary;
  reader: Subject<any>;
}

export interface FilePayload {
  fileId: number;
  buffer: ArrayBuffer;
}

export interface FileRequestPayload {
  id: number;
  offset: number;
}
