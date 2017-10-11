import {Writable, WritableOptions} from 'stream';
import autobind from 'class-autobind';
import {MessageType} from './message-type';
import * as WebSocket from 'ws';

export { Writable, WritableOptions };

export interface BinarySenderOptions {
  opId: number;
  fileId: number;
  socket: WebSocket;
}

export class BinarySender extends Writable {
  private static readonly HEADER_SIZE = 16;
  private buf: Buffer;
  private bufOffset: number;
  private seq: number;
  private opId: number;
  private fileId: number;
  private socket: WebSocket;

  constructor(opts: BinarySenderOptions, streamOpts?: WritableOptions) {
    super(streamOpts);
    autobind(this);
    let bufSize;
    if (streamOpts && streamOpts.highWaterMark) {
      bufSize = streamOpts.highWaterMark;
    } else {
      bufSize = 200 * 1024;
    }
    this.buf = new Buffer(BinarySender.HEADER_SIZE + bufSize);
    this.bufOffset = BinarySender.HEADER_SIZE;
    this.seq = 0;
    this.socket = opts.socket;
    this.fileId = opts.fileId;
    this.opId = opts.opId;

    this.buf.writeUInt32LE(this.opId, 0);
    this.buf.writeUInt32LE(MessageType.GQL_BINARY, 4);
    this.buf.writeUInt32LE(this.fileId, 8);
  }

  public async _write(chunk: Buffer, encoding?: string, callback?: Function) {
    let chunkOffset = 0;
    while (chunkOffset < chunk.length) {
      const len = Math.min(this.buf.length - this.bufOffset, chunk.length - chunkOffset);
      const written = chunk.copy(this.buf, this.bufOffset, chunkOffset, chunkOffset + len);
      chunkOffset += written;
      this.bufOffset += written;

      if (this.bufOffset === this.buf.length) {
        try {
          await this.sendData();
          this.bufOffset = BinarySender.HEADER_SIZE;
        } catch (err) {
          callback(err);
          return;
        }
      }
    }
    callback();
  }

  public async _final(callback?: Function) {
    try {
      const sent = await this.sendData();
      if (sent > 0) { // SEND EOS
        this.bufOffset = BinarySender.HEADER_SIZE;
        await this.sendData();
      }
    } catch (err) {
      callback(err);
      return;
    }
    callback();
  }

  private async waitAck() {
    return new Promise(resolve => {
      const onMessage = ({ data }: { data: ArrayBuffer }) => {
        const message = Buffer.from(data);
        let id, fileId, seq;
        // TODO: Check buffer size before accessing it
        id = message.readUInt32LE(0);
        const type = message.readUInt32LE(4);
        if (type === MessageType.GQL_BINARY_ACK) {
          fileId = message.readUInt32LE(8);
          seq = message.readUInt32LE(12);
        }
        if (id === this.opId && fileId === this.fileId && seq === this.seq) {
          this.socket.removeListener('message', onMessage);
          resolve();
        }
      };
      this.socket.addEventListener('message', onMessage);
    });
  }

  private async sendData() {
    this.seq++;
    this.buf.writeUInt32LE(this.seq, 12);
    let buf = this.buf.buffer;
    if (this.bufOffset < this.buf.length) {
      buf = this.buf.buffer.slice(0, this.bufOffset);
    }
    const ack = this.waitAck();
    await new Promise((resolve, reject) =>
      this.socket.send(buf, (err) => err ? reject(err) : resolve()));
    await ack;
    return buf.byteLength;
  }
}
