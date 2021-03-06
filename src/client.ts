declare let window: any;
const _global = typeof global !== 'undefined' ? global : (typeof window !== 'undefined' ? window : {});
const NativeWebSocket = _global.WebSocket || _global.MozWebSocket;

import * as Backoff from 'backo2';
import { EventEmitter, ListenerFn } from 'eventemitter3';
import isString = require('lodash.isstring');
import isObject = require('lodash.isobject');
import { ExecutionResult } from 'graphql/execution/execute';
import { print } from 'graphql/language/printer';
import { DocumentNode } from 'graphql/language/ast';
import { getOperationAST } from 'graphql/utilities/getOperationAST';
import $$observable from 'symbol-observable';

import { GRAPHQL_WS } from './protocol';
import { WS_TIMEOUT } from './defaults';
import { MessageType } from './message-type';
import {Binary} from './Binary';
import {BinarySender} from './BinarySender';
import {
  buildMessage,
  extractIncomingFiles, extractOutgoingFiles, FileRequestPayload, parseMessage, SocketWrapper,
} from './common';

export { Readable, Writable, Buffer } from 'readable-stream';

export { Binary };

export interface Observer<T> {
  next?: (value: T) => void;
  error?: (error: Error) => void;
  complete?: () => void;
}

export interface Observable<T> {
  subscribe(observer: Observer<T>): {
    unsubscribe: () => void;
  };
}

export interface OperationOptions {
  query?: string | DocumentNode;
  variables?: Object;
  operationName?: string;
  [key: string]: any;
}

export type FormatedError = Error & {
  originalError?: any;
};

export interface Operation {
  options: OperationOptions;
  handler: (error: Error[], result?: any) => void;
}

export interface Operations {
  [id: number]: Operation;
}

export interface OperationFiles {
  [id: number]: Binary[];
}

export interface Middleware {
  applyMiddleware(options: OperationOptions, next: Function): void;
}

export type ConnectionParams = {
  [paramName: string]: any,
};

export type ConnectionParamsOptions = ConnectionParams | Function;

export interface ClientOptions {
  connectionParams?: ConnectionParamsOptions;
  timeout?: number;
  reconnect?: boolean;
  reconnectionAttempts?: number;
  connectionCallback?: (error: Error[], result?: any) => void;
  lazy?: boolean;
}

export class SubscriptionClient {
  public client: SocketWrapper;
  public operations: Operations;
  private filesOut: OperationFiles;
  private filesIn: OperationFiles;
  private url: string;
  private nextOperationId: number;
  private connectionParams: ConnectionParamsOptions;
  private wsTimeout: number;
  private unsentMessagesQueue: Array<any>; // queued messages while websocket is opening.
  private reconnect: boolean;
  private reconnecting: boolean;
  private reconnectionAttempts: number;
  private backoff: any;
  private connectionCallback: any;
  private eventEmitter: EventEmitter;
  private lazy: boolean;
  private closedByUser: boolean;
  private wsImpl: any;
  private wasKeepAliveReceived: boolean;
  private tryReconnectTimeoutId: any;
  private checkConnectionIntervalId: any;
  private maxConnectTimeoutId: any;
  private middlewares: Middleware[];
  private maxConnectTimeGenerator: any;

  constructor(url: string, options?: ClientOptions, webSocketImpl?: any) {
    const {
      connectionCallback = undefined,
      connectionParams = {},
      timeout = WS_TIMEOUT,
      reconnect = false,
      reconnectionAttempts = Infinity,
      lazy = false,
    } = (options || {});

    this.wsImpl = webSocketImpl || NativeWebSocket;

    if (!this.wsImpl) {
      throw new Error('Unable to find native implementation, or alternative implementation for WebSocket!');
    }

    this.connectionParams = connectionParams;
    this.connectionCallback = connectionCallback;
    this.url = url;
    this.operations = {};
    this.filesOut = [];
    this.filesIn = [];
    this.nextOperationId = 0;
    this.wsTimeout = timeout;
    this.unsentMessagesQueue = [];
    this.reconnect = reconnect;
    this.reconnecting = false;
    this.reconnectionAttempts = reconnectionAttempts;
    this.lazy = !!lazy;
    this.closedByUser = false;
    this.backoff = new Backoff({ jitter: 0.5 });
    this.eventEmitter = new EventEmitter();
    this.middlewares = [];
    this.client = null;
    this.maxConnectTimeGenerator = this.createMaxConnectTimeGenerator();

    if (!this.lazy) {
      this.connect();
    }
  }

  public get status() {
    if (this.client === null) {
      return this.wsImpl.CLOSED;
    }

    return this.client.readyState;
  }

  public close(isForced = true, closedByUser = true) {
    if (this.client !== null) {
      this.closedByUser = closedByUser;

      if (isForced) {
        this.clearCheckConnectionInterval();
        this.clearMaxConnectTimeout();
        this.clearTryReconnectTimeout();
        this.unsubscribeAll();
        this.sendMessage(undefined, MessageType.GQL_CONNECTION_TERMINATE, null);
      }

      this.client.close();
      this.client = null;
      this.eventEmitter.emit('disconnected');

      if (!isForced) {
        this.tryReconnect();
      }
    }
  }

  public request(request: OperationOptions): Observable<ExecutionResult> {
    const getObserver = this.getObserver.bind(this);
    const executeOperation = this.executeOperation.bind(this);
    const unsubscribe = this.unsubscribe.bind(this);

    let opId: number;

    return {
      [$$observable]() {
        return this;
      },
      subscribe(
        observerOrNext: ((Observer<ExecutionResult>) | ((v: ExecutionResult) => void)),
        onError?: (error: Error) => void,
        onComplete?: () => void,
      ) {
        const observer = getObserver(observerOrNext, onError, onComplete);

        opId = executeOperation(request, (error: Error[], result: any) => {
          if ( error === null && result === null ) {
            if ( observer.complete ) {
              observer.complete();
            }
          } else if (error) {
            if ( observer.error ) {
              observer.error(error[0]);
            }
          } else {
            if ( observer.next ) {
              observer.next(result);
            }
          }
        });

        return {
          unsubscribe: () => {
            if ( opId ) {
              unsubscribe(opId);
              opId = null;
            }
          },
        };
      },
    };
  }

  public on(eventName: string, callback: ListenerFn, context?: any): Function {
    const handler = this.eventEmitter.on(eventName, callback, context);

    return () => {
      handler.off(eventName, callback, context);
    };
  }

  public onConnected(callback: ListenerFn, context?: any): Function {
    return this.on('connected', callback, context);
  }

  public onConnecting(callback: ListenerFn, context?: any): Function {
    return this.on('connecting', callback, context);
  }

  public onDisconnected(callback: ListenerFn, context?: any): Function {
    return this.on('disconnected', callback, context);
  }

  public onReconnected(callback: ListenerFn, context?: any): Function {
    return this.on('reconnected', callback, context);
  }

  public onReconnecting(callback: ListenerFn, context?: any): Function {
    return this.on('reconnecting', callback, context);
  }

  public unsubscribeAll() {
    Object.keys(this.operations).forEach( subId => {
      this.unsubscribe(parseInt(subId, 10));
    });
  }

  public applyMiddlewares(options: OperationOptions): Promise<OperationOptions> {
    return new Promise((resolve, reject) => {
      const queue = (funcs: Middleware[], scope: any) => {
        const next = (error?: any) => {
          if (error) {
            reject(error);
          } else {
            if (funcs.length > 0) {
              const f = funcs.shift();
              if (f) {
                f.applyMiddleware.apply(scope, [options, next]);
              }
            } else {
              resolve(options);
            }
          }
        };
        next();
      };

      queue([...this.middlewares], this);
    });
  }

  public use(middlewares: Middleware[]): SubscriptionClient {
    middlewares.map((middleware) => {
      if (typeof middleware.applyMiddleware === 'function') {
        this.middlewares.push(middleware);
      } else {
        throw new Error('Middleware must implement the applyMiddleware function.');
      }
    });

    return this;
  }

  private executeOperation(options: OperationOptions, handler: (error: Error[], result?: any) => void): number {
    if (this.client === null) {
      this.connect();
    }

    const opId = this.generateOperationId();
    this.operations[opId] = { options: options, handler };
    this.filesIn[opId] = [];
    this.filesOut[opId] = [];

    this.applyMiddlewares(options)
      .then(async processedOptions => {
        this.checkOperationOptions(processedOptions, handler);
        this.filesOut[opId] = extractOutgoingFiles(processedOptions.variables);
        if (this.operations[opId]) {
          this.operations[opId] = { options: processedOptions, handler };

          this.sendMessage(opId, MessageType.GQL_START, processedOptions);
        }
      })
      .catch(error => {
        this.unsubscribe(opId);
        handler(this.formatErrors(error));
      });

    return opId;
  }

  private getObserver<T>(
    observerOrNext: ((Observer<T>) | ((v: T) => void)),
    error?: (e: Error) => void,
    complete?: () => void,
  ) {
    if ( typeof observerOrNext === 'function' ) {
      return {
        next: (v: T) => observerOrNext(v),
        error: (e: Error) => error && error(e),
        complete: () => complete && complete(),
      };
    }

    return observerOrNext;
  }

  private createMaxConnectTimeGenerator() {
    const minValue = 1000;
    const maxValue = this.wsTimeout;

    return new Backoff({
      min: minValue,
      max: maxValue,
      factor: 1.2,
    });
  }

  private clearCheckConnectionInterval() {
    if (this.checkConnectionIntervalId) {
      clearInterval(this.checkConnectionIntervalId);
      this.checkConnectionIntervalId = null;
    }
  }

  private clearMaxConnectTimeout() {
    if (this.maxConnectTimeoutId) {
      clearTimeout(this.maxConnectTimeoutId);
      this.maxConnectTimeoutId = null;
    }
    }

  private clearTryReconnectTimeout() {
    if (this.tryReconnectTimeoutId) {
      clearTimeout(this.tryReconnectTimeoutId);
      this.tryReconnectTimeoutId = null;
    }
  }

  private checkOperationOptions(options: OperationOptions, handler: (error: Error[], result?: any) => void) {
    const { query, variables, operationName } = options;

    if (!query) {
      throw new Error('Must provide a query.');
    }

    if (!handler) {
      throw new Error('Must provide an handler.');
    }

    if (
      ( !isString(query) && !getOperationAST(query, operationName)) ||
      ( operationName && !isString(operationName)) ||
      ( variables && !isObject(variables))
    ) {
      throw new Error('Incorrect option types. query must be a string or a document,' +
        '`operationName` must be a string, and `variables` must be an object.');
    }
  }

  // ensure we have an array of errors
  private formatErrors(errors: any): FormatedError[] {
    if (Array.isArray(errors)) {
      return errors;
    }

    // TODO  we should not pass ValidationError to callback in the future.
    // ValidationError
    if (errors && errors.errors) {
      return this.formatErrors(errors.errors);
    }

    if (errors && errors.message) {
      return [errors];
    }

    return [{
      name: 'FormatedError',
      message: 'Unknown error',
      originalError: errors,
    }];
  }

  private sendMessage(id: number, type: number, payload: any) {
    this.sendMessageRaw(buildMessage(id, type, payload));
  }

  // send message, or queue it if connection is not open
  private sendMessageRaw(message: ArrayBuffer) {
    switch (this.status) {
      case this.wsImpl.OPEN:
        this.client.send(message);
        break;
      case this.wsImpl.CONNECTING:
        this.unsentMessagesQueue.push(message);
        break;
      default:
        if (!this.reconnecting) {
          throw new Error(
            'A message was not sent because socket is not connected, is closing or ' +
            'is already closed. ',
          );
        }
    }
  }

  private generateOperationId(): number {
    return ++this.nextOperationId;
  }

  private tryReconnect() {
    if (!this.reconnect || this.backoff.attempts >= this.reconnectionAttempts) {
      return;
    }

    if (!this.reconnecting) {
      Object.keys(this.operations).forEach((key) => {
        const opId = parseInt(key, 10);
        this.unsentMessagesQueue.push(
          buildMessage(opId, MessageType.GQL_START, this.operations[opId].options),
        );
      });
      this.reconnecting = true;
    }

    this.clearTryReconnectTimeout();

    const delay = this.backoff.duration();
    this.tryReconnectTimeoutId = setTimeout(() => {
      this.connect();
    }, delay);
  }

  private flushUnsentMessagesQueue() {
    this.unsentMessagesQueue.forEach((message) => {
      this.sendMessageRaw(message);
    });
    this.unsentMessagesQueue = [];
  }

  private checkConnection() {
    if (this.wasKeepAliveReceived) {
      this.wasKeepAliveReceived = false;
      return;
    }

    if (!this.reconnecting) {
      this.close(false, true);
    }
  }

  private checkMaxConnectTimeout() {
    this.clearMaxConnectTimeout();

    // Max timeout trying to connect
    this.maxConnectTimeoutId = setTimeout(() => {
      if (this.status !== this.wsImpl.OPEN) {
        this.close(false, true);
      }
    }, this.maxConnectTimeGenerator.duration());
  }

  private patchSocket(socket: any) {
    // Make it node.js ws compatible
    if (socket.addEventListener) {
      socket.on = (event: string, cb: any) =>
        socket.addEventListener(event, ({ data }: {data: ArrayBuffer}) => cb(data));
    }
    const sockSend = socket.send.bind(socket);
    socket.send = (data: any, callback: Function) => {
      try {
        sockSend(data);
        if (callback) {
          callback();
        }
      } catch (err) {
        if (callback) {
          callback(err);
        } else {
          throw err;
        }
      }
    };
  }

  private connect() {
    const socket = new this.wsImpl(this.url, GRAPHQL_WS);
    this.patchSocket(socket);
    this.client = new SocketWrapper(socket);
    this.client.getSocket().binaryType = 'arraybuffer';

    this.checkMaxConnectTimeout();

    this.client.getSocket().onopen = () => {
      this.clearMaxConnectTimeout();
      this.closedByUser = false;
      this.eventEmitter.emit(this.reconnecting ? 'reconnecting' : 'connecting');

      const payload: ConnectionParams = typeof this.connectionParams === 'function' ? this.connectionParams() : this.connectionParams;

      // Send CONNECTION_INIT message, no need to wait for connection to success (reduce roundtrips)
      this.sendMessage(undefined, MessageType.GQL_CONNECTION_INIT, payload);
      this.flushUnsentMessagesQueue();
    };

    this.client.getSocket().onclose = () => {
      if ( !this.closedByUser ) {
        this.close(false, false);
      }
    };

    this.client.getSocket().onerror = () => {
      // Capture and ignore errors to prevent unhandled exceptions, wait for
      // onclose to fire before attempting a reconnect.
    };

    this.client.on('message', (data: ArrayBuffer) => {
      this.processReceivedData(data);
    });
  }

  private async processReceivedData(receivedData: any) {
    const parsedMessage = parseMessage(receivedData);
    if (!parsedMessage) {
      return;
    }
    const opId = parsedMessage.id;

    if ([
        MessageType.GQL_DATA,
        MessageType.GQL_BINARY,
        MessageType.GQL_BINARY_ACK,
        MessageType.GQL_BINARY_REQUEST,
        MessageType.GQL_ERROR,
        MessageType.GQL_COMPLETE,
      ].indexOf(parsedMessage.type) !== -1 && !this.operations[opId]
    ) {
      this.unsubscribe(opId);

      return;
    }

    switch (parsedMessage.type) {
      case MessageType.GQL_CONNECTION_ERROR:
        if (this.connectionCallback) {
          this.connectionCallback(parsedMessage.payload);
        }
        break;

      case MessageType.GQL_CONNECTION_ACK:
        this.eventEmitter.emit(this.reconnecting ? 'reconnected' : 'connected');
        this.reconnecting = false;
        this.backoff.reset();
        this.maxConnectTimeGenerator.reset();

        if (this.connectionCallback) {
          this.connectionCallback();
        }
        break;

      case MessageType.GQL_COMPLETE:
        this.operations[opId].handler(null, null);
        delete this.operations[opId];
        delete this.filesOut[opId];
        break;

      case MessageType.GQL_ERROR:
        this.operations[opId].handler(this.formatErrors(parsedMessage.payload), null);
        delete this.operations[opId];
        break;

      case MessageType.GQL_DATA: {
        const parsedPayload = !parsedMessage.payload.errors ?
          parsedMessage.payload : {...parsedMessage.payload, errors: this.formatErrors(parsedMessage.payload.errors)};
        this.filesIn[opId] = extractIncomingFiles(opId, this.client, parsedPayload);
        this.operations[opId].handler(null, parsedPayload);
        break;
      }

      case MessageType.GQL_BINARY_REQUEST: {
        const payload: FileRequestPayload = (<FileRequestPayload> parsedMessage.payload);
        if (this.filesOut[opId]) {
          const { id, offset } = payload;
          const file = this.filesOut[opId].find(f => f.getId() === id);
          if (file) {
            const reader = file.createReadStream(offset);
            const writer = new BinarySender({
              opId,
              fileId: file.getId(),
              socket: this.client,
            });
            const finish = new Promise((resolve, reject) => {
              reader.pipe(writer)
                .on('finish', resolve)
                .on('error', reject);
            });
            await finish;
            if (this.filesOut[opId]) {
              const index = this.filesOut[opId].findIndex(f => f === file);
              delete this.filesOut[opId][index];
            }
          }
        }
        break;
      }

      case MessageType.GQL_CONNECTION_KEEP_ALIVE:
        const firstKA = typeof this.wasKeepAliveReceived === 'undefined';
        this.wasKeepAliveReceived = true;

        if (firstKA) {
          this.checkConnection();
        }

        if (this.checkConnectionIntervalId) {
          clearInterval(this.checkConnectionIntervalId);
          this.checkConnection();
        }
        this.checkConnectionIntervalId = setInterval(this.checkConnection.bind(this), this.wsTimeout);
        break;

      default:
        throw new Error('Invalid message type!');
    }
  }

  private unsubscribe(opId: number) {
    if (this.operations[opId]) {
      delete this.operations[opId];
      this.sendMessage(opId, MessageType.GQL_STOP, undefined);
    }
  }
}
