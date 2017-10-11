import {Readable, ReadableOptions} from 'stream';
import autobind from 'class-autobind';
import {MessageType} from './message-type';
import * as WebSocket from 'ws';
import {EventEmitter} from 'events';

export { Readable, ReadableOptions };

export interface BinaryReceiverOptions {
  offset: number;
  opId: number;
  fileId: number;
  socket: WebSocket;
}

export class BinaryReceiver extends Readable {
  private static readonly HEADER_SIZE = 16;
  private ack: Buffer;
  private opId: number;
  private fileId: number;
  private socket: WebSocket;
  private events: EventEmitter;
  private offset: number;

  constructor(opts: BinaryReceiverOptions, streamOpts?: ReadableOptions) {
    super(streamOpts);
    autobind(this);
    this.socket = opts.socket;
    this.fileId = opts.fileId;
    this.opId = opts.opId;
    this.offset = opts.offset;
    this.events = new EventEmitter();

    this.ack = new Buffer(BinaryReceiver.HEADER_SIZE);
    this.ack.writeUInt32LE(this.opId, 0);
    this.ack.writeUInt32LE(MessageType.GQL_BINARY_ACK, 4);
    this.ack.writeUInt32LE(this.fileId, 8);

    this.startReceiving();
  }

  public async _read(size?: number) {
    // TODO: Check if size really matters ( check console.log(size) )
    process.nextTick(() => this.events.emit('read', size));
  }

  private startReceiving() {
    const req = new Buffer(BinaryReceiver.HEADER_SIZE);
    req.writeUInt32LE(this.opId, 0);
    req.writeUInt32LE(MessageType.GQL_BINARY_REQUEST, 4);
    req.writeUInt32LE(this.fileId, 8);
    req.writeUInt32LE(this.offset, 12);
    this.socket.send(req.buffer, (err) => {
      if (err) {
        process.nextTick(() => this.emit('error', err));
      } else {
        this.socket.addEventListener('message', this.onMessage);
      }
    });
  }

  private stopReceiving() {
    this.socket.removeListener('message', this.onMessage);
  }

  private async onMessage({ data }: { data: ArrayBuffer }) {
    const buf = Buffer.from(data);
    let id, fileId, seq;
    // TODO: Check buffer size before accessing it
    id = buf.readUInt32LE(0);
    const type = buf.readUInt32LE(4);
    if (type === MessageType.GQL_BINARY) {
      fileId = buf.readUInt32LE(8);
      seq = buf.readUInt32LE(12);
    }
    if (id !== this.opId || fileId !== this.fileId) {
      return;
    }
    const payload = buf.slice(BinaryReceiver.HEADER_SIZE, buf.length);
    if (!payload.length) {
      this.stopReceiving();
      this.push(null);
    } else {
      if (!this.push(payload)) {
        const read = new Promise(resolve => this.events.once('read', resolve));
        await read;
      }
    }
    try {
      await this.sendAck(seq);
    } catch (err) {
      this.stopReceiving();
      process.nextTick(() => this.emit('error', err));
    }
  }

  private async sendAck(seq: number) {
    this.ack.writeUInt32LE(seq, 12);
    return new Promise((resolve, reject) =>
      this.socket.send(this.ack.buffer, (err) => err ? reject(err) : resolve()));
  }
}
