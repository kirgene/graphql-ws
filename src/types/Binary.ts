import { EventEmitter } from 'events';

export type DataFunction = (data: Buffer | ArrayBuffer) => Promise<any>;
export type CompleteFunction = (error?: any) => Promise<any>;
export type BinaryReaderFunction = (resource: any, offset: number, onData: DataFunction, onComplete: CompleteFunction) => void;

// TODO: Add totalSize field to track progress (only!, without detecting EOS) on client/server
export interface SerializedBinary {
  ___binary: true;
  id: number;
  hash: string;
}

class BinaryError extends Error {
  constructor(...args: any[]) {
    super(...args);
  }
}

export class Binary {
  private static _id: number = 0;
  // 1MB, make it larger than chunk size (like 10MB) or similar
  private static readonly MAX_BUFFER_SIZE: number = 1024 * 1024;
  private size: number;
  private buf: any[];
  private bufPos: number;
  private resource: any;
  private reader: any;
  private event: EventEmitter;
  private eos: Symbol;
  private hash: string;
  private id: number;

  public static isBinary(obj: Object) {
    return obj && (typeof obj === 'object') && obj.hasOwnProperty('___binary');
  }

  constructor(resource: any, reader: BinaryReaderFunction, serialized?: SerializedBinary) {
    this.size = 0;
    this.buf = [];
    this.bufPos = 0;
    this.resource = resource;
    this.reader = reader;
    this.event = new EventEmitter();
    this.eos = Symbol('EOS');
    if (serialized && serialized.___binary) {
      this.hash = serialized.hash;
      this.id = serialized.id;
    } else {
      this.hash = null;
      this.id = ++Binary._id;
    }
  }
  public startRead(offset = 0) {
    this.size = 0;
    this.buf = [];
    this.bufPos = 0;
    this.reader(this.resource, offset, this.onData.bind(this), this.onComplete.bind(this));
  }
  public async readInto(dest: ArrayBuffer, size: number, offset = 0): Promise<number> {
    if (size > Binary.MAX_BUFFER_SIZE) {
      throw new Error('Requested read size is larger than internal buffer size');
    }
    let read = 0;
    while (read < size) {
      while (this.buf.length === 0) {
        await this.waitForData();
      }
      if (this.buf[0] === this.eos) {
        break;
      }
      if (this.buf[0] instanceof BinaryError) {
        throw this.buf[0];
      }
      const leftToRead = size - read;
      let arraySize = (this.buf[0].byteLength - this.bufPos);
      if (arraySize > leftToRead) {
        arraySize = leftToRead;
      }
      let buffer = this.buf[0];
      if (buffer instanceof Buffer) {
        buffer = new Uint8Array(this.buf[0]).buffer;
      }
      const array = new Uint8Array(buffer, this.bufPos, arraySize);
      if (array.byteLength !== arraySize) {
        throw new Error('array.byteLength != arraySize');
      }
      new Uint8Array(dest).set(array, offset + read);
      if (this.buf[0].byteLength === (this.bufPos + array.byteLength)) {
        this.size -= this.buf[0].byteLength;
        this.buf.shift(); // Remove processed element
        this.bufPos = 0;
        this.event.emit('removed');
      }
      // TODO: REMOVE THIS BLOCK WHEN TESTED
      else if (this.buf[0].byteLength < (this.bufPos + array.byteLength)) {
        console.log('BUG: this.buf[0].byteLength < (this.bufPos + array.byteLength');
      }
      else {
        this.bufPos += array.byteLength;
      }
      read += array.byteLength;
    }
    return read;
  }
  public getHash() {
    return this.hash;
  }
  public setHash(hash: string) {
    this.hash = hash;
  }
  public getId() {
    return this.id;
  }
  public equal(binary: Binary) {
    return this.hash === binary.hash;
  }
  public clone(source: Binary) {
    this.size = source.size;
    this.buf = source.buf;
    this.bufPos = source.bufPos;
    this.resource = source.resource;
    this.reader = source.reader;
    this.event = source.event;
    this.eos = source.eos;
    this.hash = source.hash;
    this.id = source.id;
  }
  public toJSON() {
    return { ___binary: true, id: this.id, hash: this.hash };
  }
  private isValidData(data: any) {
    return (data instanceof Buffer) ||
      (data instanceof ArrayBuffer) ||
      (data === this.eos) ||
      (data instanceof BinaryError);
  }
  private async onData(data: any) {
    if (this.isValidData(data)) {
      this.buf.push(data);
    } else {
      console.log('Unexpected data type: Buffer or ArrayBuffer expected');
      return;
    }
    if (data) {
      this.size += data.byteLength;
    }
    this.event.emit('added');
    while (this.size > Binary.MAX_BUFFER_SIZE) {
      await this.waitForSpace();
    }
  }
  private async onComplete(error?: any) {
    if (error) {
      return this.onData(new BinaryError(error));
    } else {
      return this.onData(this.eos);
    }
  }
  private async waitForData() {
    return new Promise(resolve => this.event.once('added', resolve));
  }
  private async waitForSpace() {
    return new Promise(resolve => this.event.once('removed', resolve));
  }
}
