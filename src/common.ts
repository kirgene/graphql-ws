import {Binary, SerializedBinary} from './Binary';
import {BinaryReceiver} from './BinaryReceiver';
import {MessageType} from './message-type';
import {EventEmitter} from 'events';

export interface FileRequestPayload {
  id: number;
  offset: number;
}

export class SocketWrapper implements SocketLike {
  private events: EventEmitter;
  private socket: any;

  constructor(socket: any) {
    this.events = new EventEmitter();
    this.socket = socket;
    this.socket.on('message', this.onMessage.bind(this));
  }

  public send(data: any, cb?: (err: Error) => void): void {
    this.socket.send(data, cb);
  }

  public get readyState(): number {
    return this.socket.readyState;
  }

  public close(code?: number, data?: any): void {
    this.socket.close(code, data);
  }

  public getSocket(): any {
    return this.socket;
  }

  public on(event: 'message', cb: (data: any, flags: { binary: boolean }) => void): this {
    this.events.on('message', cb);
    return this;
  }

  public once(event: 'message', cb: (data: any, flags: { binary: boolean }) => void): this {
    this.events.once('message', cb);
    return this;
  }

  public removeListener(event: 'message', cb: (data: any, flags: { binary: boolean }) => void): this {
    this.events.removeListener('message', cb);
    return this;
  }

  public removeEventListener(event: 'message', cb: (data: any, flags: { binary: boolean }) => void): this {
    this.events.removeListener('message', cb);
    return this;
  }

  private onMessage(message: ArrayBuffer) {
    this.events.emit('message', message);
  }
}

export interface SocketLike {
  send: (data: any, cb?: (err: Error) => void) => void;
  on: Function;
  once: Function;
  removeListener: Function;
}

export function repeatPromise(promise: () => Promise<boolean>): any {
    return promise().then((repeat: boolean) => repeat && repeatPromise(promise));
}

function findBinaries(object: any): Binary[] {
  const value: Binary[] = [];
  Object.keys(object || {}).forEach((k) => {
    if (object[k] instanceof Binary) {
      value.push(object[k]);
    } else if (object[k] && typeof object[k] === 'object') {
      value.push(...findBinaries(object[k]));
    }
  });
  return value;
}

function deserializeBinaries(
  object: any,
  callback: (value: SerializedBinary) => Binary,
) {
  Object.keys(object).forEach((k) => {
    if (Binary.isBinary(object[k])) {
      object[k] = callback(object[k]);
    } else if (object[k] && typeof object[k] === 'object') {
      deserializeBinaries(object[k], callback);
    }
  });
}

export function extractIncomingFiles(opId: number, socket: any, obj?: { [key: string]: any }): Binary[] {
  const files: Binary[] = [];
  deserializeBinaries(obj || {}, (file) => {
    const onStream = (offset: number) => new BinaryReceiver({
      opId,
      fileId: file.id,
      offset,
      socket,
    });
    const binary = new Binary(onStream, file);
    const found = files.find(f => f.equal(binary));
    if (found) {
      binary.clone(found);
    } else {
      files.push(binary);
    }
    return binary;
  });
  return files;
}

export function extractOutgoingFiles(obj?: { [key: string]: any }): Binary[] {
  const files: Binary[] = [];
  for (let file of findBinaries(obj || {})) {
    const found = files.find(f => f.equal(file));
    if (found) {
      file.clone(found);
    } else {
      files.push(file);
    }
  }
  return files;
}

export function parseMessage(buffer: ArrayBuffer) {
  const message = Buffer.from(buffer);
  let result;
  // TODO: Check buffer size before accessing it
  let payloadBase = {
    id: message.readUInt32LE(0),
    type: message.readUInt32LE(4),
  };
  if (payloadBase.type === MessageType.GQL_BINARY_REQUEST) {
    const payload: FileRequestPayload = {
      id: message.readUInt32LE(8),
      offset: message.readUInt32LE(12),
    };
    result = {
      ...payloadBase,
      payload,
    };
  } else {
    const availableTypes = [
      MessageType.GQL_CONNECTION_INIT,
      MessageType.GQL_START,
      MessageType.GQL_STOP,
      MessageType.GQL_CONNECTION_TERMINATE,
      MessageType.GQL_CONNECTION_ERROR,
      MessageType.GQL_CONNECTION_ACK,
      MessageType.GQL_COMPLETE,
      MessageType.GQL_ERROR,
      MessageType.GQL_DATA,
      MessageType.GQL_CONNECTION_KEEP_ALIVE,
    ];
    if (availableTypes.includes(payloadBase.type)) {
      const payloadStr = Buffer.from(buffer, 8).toString();
      const payload = payloadStr.length > 0 ? JSON.parse(payloadStr) : null;
      result = {
        ...payloadBase,
        payload,
      };
    }
  }
  return result;
}

export function buildMessage(id: number, type: number, payload: any): ArrayBuffer {
  const serializedMessage = Buffer.from(JSON.stringify(payload) || '');
  const headerSize = 8;
  const message = new Buffer(headerSize + serializedMessage.length);
  message.writeUInt32LE(id, 0);
  message.writeUInt32LE(type, 4);
  serializedMessage.copy(message, headerSize);
  return message.buffer as ArrayBuffer;
}
