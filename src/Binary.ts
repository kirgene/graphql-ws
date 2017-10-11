import {Readable} from 'stream';

export type StreamFunction = (offset: number) => Readable;

export interface SerializedBinary {
  ___binary: true;
  id: number;
  hash: string;
  size: number;
}

export interface BinaryOptions {
  hash: string;
  size: number;
}

export class Binary {
  private static _id: number = 0;
  private onStream: StreamFunction;
  private stream: Readable;
  private id: number;
  private hash: string;
  private size: number;

  public static isBinary(obj: Object) {
    return obj && (typeof obj === 'object') && obj.hasOwnProperty('___binary');
  }

  constructor(stream: StreamFunction, options: BinaryOptions | SerializedBinary) {
    this.onStream = stream;
    this.stream = null;
    this.hash = options.hash;
    this.size = options.size;
    if (Binary.isBinary(options)) {
      this.id = (options as SerializedBinary).id;
    } else {
      this.id = ++Binary._id;
    }
  }
  public createReadStream(offset: number = 0): Readable {
    return this.stream = this.onStream(offset);
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
  public getStream(): Readable {
    return this.stream;
  }
  public equal(binary: Binary) {
    return (this === binary) || (this.hash && binary.hash && this.hash === binary.hash);
  }
  public clone(source: Binary) {
    this.hash = source.hash;
    this.size = source.size;
    this.id = source.id;
    this.onStream = source.onStream;
    this.stream = source.stream;
  }
  public toJSON(): SerializedBinary {
    return {
      ___binary: true,
      id: this.id,
      hash: this.hash,
      size: this.size,
    };
  }
}
