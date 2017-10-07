import { EventEmitter } from 'events';

export type DataFunction = (data: Buffer | ArrayBuffer) => Promise<any>;
export type CompleteFunction = (error?: any) => Promise<any>;
export type BinaryReadFunction = (resource: any, offset: number, onData: DataFunction, onComplete: CompleteFunction) => void;
export type BinaryHashFunction = (resource: any) => Promise<string>;
export type BinarySizeFunction = (resource: any) => Promise<number>;

export interface SerializedBinary {
  ___binary: true;
  id: number;
  hash: string;
  size: number;
}

class BinaryError extends Error {
  constructor(...args: any[]) {
    super(...args);
  }
}

export interface BinaryOptions {
  onRead: BinaryReadFunction;
  onHash?: BinaryHashFunction;
  onSize?: BinarySizeFunction;
}

export class Binary {
  private static _id: number = 0;
  // 1MB, make it larger than chunk size (like 10MB) or similar
  private static readonly MAX_BUFFER_SIZE: number = 1024 * 1024;
  private buf: any[];
  private bufPos: number;
  private bufSize: number;
  private resource: any;
  private onRead: BinaryReadFunction;
  private onHash: BinaryHashFunction;
  private onSize: BinarySizeFunction;
  private event: EventEmitter;
  private eos: Symbol;
  private hash: string;
  private id: number;
  private size: number;

  public static isBinary(obj: Object) {
    return obj && (typeof obj === 'object') && obj.hasOwnProperty('___binary');
  }

  constructor(resource: any, options: BinaryOptions, serialized?: SerializedBinary) {
    this.buf = [];
    this.bufPos = 0;
    this.bufSize = 0;
    this.eos = Symbol('EOS');
    this.resource = resource;
    this.onRead = options.onRead;
    this.onSize = options.onSize;
    this.onHash = options.onHash;
    this.event = new EventEmitter();
    if (serialized && serialized.___binary) {
      this.hash = serialized.hash;
      this.id = serialized.id;
      this.size = serialized.size;
    } else {
      this.hash = null;
      this.id = ++Binary._id;
      this.size = 0;
    }
  }
  public initRead(offset = 0) {
    this.buf = [];
    this.bufPos = 0;
    this.bufSize = 0;
    this.onRead(this.resource, offset, this.onData.bind(this), this.onComplete.bind(this));
  }
  public async initMetadata() {
    if (!this.hash) {
      if (!this.onHash) {
        throw new Error('hash and onHash callback not defined');
      } else {
        this.hash = await this.onHash(this.resource);
      }
    }
    if (!this.size) {
      if (!this.onSize) {
        throw new Error('size and onSize callback not defined');
      } else {
        this.size = await this.onSize(this.resource);
      }
    }
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
        this.bufSize -= this.buf[0].byteLength;
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
  public getHash(): string {
    return this.hash;
  }
  public getSize(): number {
    return this.size;
  }
  public getId(): number {
    return this.id;
  }
  public equal(binary: Binary) {
    return (this === binary) || (this.hash && binary.hash && this.hash === binary.hash);
  }
  public clone(source: Binary) {
    this.buf = source.buf;
    this.bufPos = source.bufPos;
    this.bufSize = source.bufSize;
    this.resource = source.resource;
    this.onRead = source.onRead;
    this.onHash = source.onHash;
    this.onSize = source.onSize;
    this.event = source.event;
    this.eos = source.eos;
    this.hash = source.hash;
    this.size = source.size;
    this.id = source.id;
  }
  public toJSON(): SerializedBinary {
    return {
      ___binary: true,
      id: this.id,
      hash: this.hash,
      size: this.size,
    };
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
      throw new Error('Unexpected data type: Buffer or ArrayBuffer expected');
    }
    if (data) {
      this.bufSize += data.byteLength;
    }
    this.event.emit('added');
    while (this.bufSize > Binary.MAX_BUFFER_SIZE) {
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
