import * as WebSocket from 'ws';
import { EventEmitter } from 'events';

import { MessageType } from './message-type';
import { GRAPHQL_WS } from './protocol';
import isObject = require('lodash.isobject');
import {
  parse,
  ExecutionResult,
  GraphQLSchema,
  DocumentNode,
  validate,
  ValidationContext,
  specifiedRules,
  GraphQLFieldResolver,
} from 'graphql';
import { createEmptyIterable } from './utils/empty-iterable';
import { createAsyncIterator, forAwaitEach, isAsyncIterable } from 'iterall';
import { createIterableFromPromise } from './utils/promise-to-iterable';
import { isASubscriptionOperation } from './utils/is-subscriptions';
import { IncomingMessage } from 'http';
import { Subject } from 'rxjs';
import * as crypto from 'crypto';
import {Binary, DataFunction, CompleteFunction, SerializedBinary} from './types/Binary';
import {
  BINARY_CHUNK_SIZE, FilePayload, FileRequestPayload, findBinaries, IncomingFile,
  deserializeBinaries
} from './common';

export type ExecutionIterator = AsyncIterator<ExecutionResult>;

export interface ExecutionParams<TContext = any> {
  query: string | DocumentNode;
  variables: { [key: string]: any };
  operationName: string;
  context: TContext;
  formatResponse?: Function;
  formatError?: Function;
  callback?: Function;
}

export type ConnectionContext = {
  initPromise?: Promise<any>,
  socket: WebSocket,
  filesIn: {
    [id: number]: IncomingFile[],
  }
  filesOut: {
    [id: number]: Binary[],
  }
  filesOutEvent: EventEmitter;
  operations: {
    [opId: number]: ExecutionIterator,
  },
};

export interface QueryPayload {
  [key: string]: any; // this will support for example any options sent in init like the auth token
  query?: string;
  variables?: { [key: string]: any };
  operationName?: string;
}

export interface OperationMessage {
  payload?: QueryPayload | FilePayload | FileRequestPayload;
  id?: string;
  type: number;
}

export type ExecuteFunction = (schema: GraphQLSchema,
                               document: DocumentNode,
                               rootValue?: any,
                               contextValue?: any,
                               variableValues?: { [key: string]: any },
                               operationName?: string,
                               fieldResolver?: GraphQLFieldResolver<any, any>) =>
                               Promise<ExecutionResult> |
                               AsyncIterator<ExecutionResult>;

export type SubscribeFunction = (schema: GraphQLSchema,
                                 document: DocumentNode,
                                 rootValue?: any,
                                 contextValue?: any,
                                 variableValues?: { [key: string]: any },
                                 operationName?: string,
                                 fieldResolver?: GraphQLFieldResolver<any, any>,
                                 subscribeFieldResolver?: GraphQLFieldResolver<any, any>) =>
                                 AsyncIterator<ExecutionResult> |
                                 Promise<AsyncIterator<ExecutionResult> | ExecutionResult>;

export interface ServerOptions {
  rootValue?: any;
  schema?: GraphQLSchema;
  execute?: ExecuteFunction;
  subscribe?: SubscribeFunction;
  validationRules?: Array<(context: ValidationContext) => any>;

  onOperation?: Function;
  onOperationComplete?: Function;
  onConnect?: Function;
  onDisconnect?: Function;
  keepAlive?: number;
}

export class SubscriptionServer {
  private onOperation: Function;
  private onOperationComplete: Function;
  private onConnect: Function;
  private onDisconnect: Function;

  private wsServer: WebSocket.Server;
  private execute: ExecuteFunction;
  private subscribe: SubscribeFunction;
  private schema: GraphQLSchema;
  private rootValue: any;
  private keepAlive: number;
  private closeHandler: () => void;
  private specifiedRules: Array<(context: ValidationContext) => any>;

  public static create(options: ServerOptions, socketOptions: WebSocket.IServerOptions) {
    return new SubscriptionServer(options, socketOptions);
  }

  private static async calcBinaryHash(file: Binary) {
    // Check if hash was already provided
    if (file.getHash()) {
      return;
    }
    // TODO: Use web worker (see below comment block)
    const hash = crypto.createHash('sha256');
    file.startRead();
    const buffer = new ArrayBuffer(BINARY_CHUNK_SIZE);
    let read: number;
    do {
      read = await file.readInto(buffer, BINARY_CHUNK_SIZE);
      hash.update(Buffer.from(buffer, 0, read));
    } while (read === BINARY_CHUNK_SIZE);
    file.setHash(hash.digest('base64'));

    /*
    return new Promise((resolve, reject) => {
      const worker = new Worker('CalcMD5Worker.js');
      worker.postMessage([f]);
      worker.onmessage = (e) => resolve(e.data.md5);
      worker.onerror = reject;
    });
    */
  }

  constructor(options: ServerOptions, socketOptions: WebSocket.IServerOptions) {
    const {
      onOperation, onOperationComplete, onConnect, onDisconnect, keepAlive,
    } = options;

    this.specifiedRules = options.validationRules || specifiedRules;
    this.loadExecutor(options);

    this.onOperation = onOperation;
    this.onOperationComplete = onOperationComplete;
    this.onConnect = onConnect;
    this.onDisconnect = onDisconnect;
    this.keepAlive = keepAlive;

    // Init and connect websocket server to http
    this.wsServer = new WebSocket.Server(socketOptions || {});

    const connectionHandler = ((socket: WebSocket, request: IncomingMessage) => {
      // Add `upgradeReq` to the socket object to support old API, without creating a memory leak
      // See: https://github.com/websockets/ws/pull/1099
      (socket as any).upgradeReq = request;
      (socket as any).binaryType = 'arraybuffer';
      if (socket.protocol === undefined ||
        (socket.protocol.indexOf(GRAPHQL_WS) === -1)) {
        // Close the connection with an error code, ws v2 ensures that the
        // connection is cleaned up even when the closing handshake fails.
        // 1002: protocol error
        socket.close(1002);

        return;
      }

      const connectionContext: ConnectionContext = Object.create(null);
      connectionContext.socket = socket;
      connectionContext.operations = {};
      connectionContext.filesIn = {};
      connectionContext.filesOut = {};
      connectionContext.filesOutEvent = new EventEmitter();

      // Regular keep alive messages if keepAlive is set
      if (this.keepAlive) {
        const keepAliveTimer = setInterval(() => {
          if (socket.readyState === WebSocket.OPEN) {
            this.sendMessage(connectionContext, undefined, MessageType.GQL_CONNECTION_KEEP_ALIVE, undefined);
          } else {
            clearInterval(keepAliveTimer);
          }
        }, this.keepAlive);
      }

      const connectionClosedHandler = (error: any) => {
        if (error) {
          this.sendError(
            connectionContext,
            0,
            { message: error.message ? error.message : error },
            MessageType.GQL_CONNECTION_ERROR,
          );

          setTimeout(() => {
            // 1011 is an unexpected condition prevented the request from being fulfilled
            connectionContext.socket.close(1011);
          }, 10);
        }
        this.onClose(connectionContext);

        if (this.onDisconnect) {
          this.onDisconnect(socket);
        }
      };

      socket.on('error', connectionClosedHandler);
      socket.on('close', connectionClosedHandler);
      socket.on('message', this.onMessage(connectionContext));
    });

    this.wsServer.on('connection', connectionHandler);
    this.closeHandler = () => {
      this.wsServer.removeListener('connection', connectionHandler);
      this.wsServer.close();
    };
  }

  public get server(): WebSocket.Server {
    return this.wsServer;
  }

  public close(): void {
    this.closeHandler();
  }

  private loadExecutor(options: ServerOptions) {
    const { execute, subscribe, schema, rootValue } = options;

    if (!execute) {
      throw new Error('Must provide `execute` for websocket server constructor.');
    }

    if (!schema) {
      throw new Error('`schema` is missing');
    }

//    Object.assign(schema.getTypeMap(), File);

    this.schema = schema;
    this.rootValue = rootValue;
    this.execute = execute;
    this.subscribe = subscribe;
  }

  private unsubscribe(connectionContext: ConnectionContext, opId: number) {
    if (connectionContext.operations && connectionContext.operations[opId]) {
      if (connectionContext.operations[opId].return) {
        connectionContext.operations[opId].return();
      }

      delete connectionContext.operations[opId];
      delete connectionContext.filesIn[opId];
      delete connectionContext.filesOut[opId];

      if (this.onOperationComplete) {
        this.onOperationComplete(connectionContext.socket, opId);
      }
    }
  }

  private onClose(connectionContext: ConnectionContext) {
    Object.keys(connectionContext.operations).forEach((opId) => {
      this.unsubscribe(connectionContext, parseInt(opId, 10));
    });
  }

  private parseMessage(buffer: any) {
    const message = new DataView(buffer);
    let result;
    // TODO: Check buffer size before accessing it
    let payloadBase = {
      id: message.getUint32(0, true),
      type: message.getUint32(4, true),
    };
    if (payloadBase.type === MessageType.GQL_BINARY) {
      const payload: FilePayload = {
        fileId: message.getUint32(8, true),
        buffer: buffer.slice(12),
      };
      result = {
        ...payloadBase,
        payload,
      };
    } else {
      const payloadStr = Buffer.from(buffer, 8).toString();
      const payload: QueryPayload = payloadStr.length > 0 ? JSON.parse(payloadStr) : null;
      result = {
        ...payloadBase,
        payload,
      };
    }
    return result;
  }

  private binaryReader(resource: any, offset: number, onData: DataFunction, onComplete: CompleteFunction) {
    const { connectionContext, opId, fileId } = resource;
    const file = connectionContext.filesIn[opId].find((f: IncomingFile) => f.binary.getId() === fileId);
    const reader = file.reader;
    const request = {
      id: fileId,
      offset,
    };
    this.sendMessage(connectionContext, opId, MessageType.GQL_BINARY_REQUEST, request);
    reader.subscribe(
      async (data: ArrayBuffer) => onData(data),
      async (error: any) => onComplete(error),
      async () => onComplete(),
      );
  }

  private processFiles(ctx: ConnectionContext, opId: number, variables?: { [key: string]: any }) {
    deserializeBinaries(variables || {}, (file) => {
      const obj = { connectionContext: ctx, opId, fileId: file.id };
      const binary = new Binary(obj, this.binaryReader.bind(this), file);
      const found = ctx.filesIn[opId].find(f => f.binary.equal(binary));
      if (found) {
        binary.clone(found.binary);
      } else {
        const reader = new Subject();
        ctx.filesIn[opId].push({
          binary,
          reader,
        });
      }
      return binary;
    });
  }

  private async extractFiles(response?: { [key: string]: any }): Promise<Binary[]> {
    const files: Binary[] = [];
    for (let file of findBinaries(response || {})) {
      await SubscriptionServer.calcBinaryHash(file);
      const found = files.find((f: Binary) => f.equal(file));
      if (!found) {
        files.push(file);
      } else {
        file.clone(found);
      }
    }
    return files;
  }

  private async sendSingleFile(connectionContext: ConnectionContext, opId: number, file: Binary, offset = 0) {
    const headerSize = 4 * 3;
    const chunkSize = BINARY_CHUNK_SIZE;

    const buffer = new DataView(new ArrayBuffer(headerSize + chunkSize));
    buffer.setUint32(0, opId, true);
    buffer.setUint32(4, MessageType.GQL_BINARY, true);
    buffer.setUint32(8, file.getId(), true);

    file.startRead();
    let read: number;
    do {
      read = await file.readInto(buffer.buffer, chunkSize, headerSize);
      if (connectionContext.socket.readyState !== WebSocket.OPEN) {
        break;
      }
      let buf = buffer.buffer;
      if (read < chunkSize) {
        buf = buffer.buffer.slice(0, headerSize + read);
      }
      connectionContext.socket.send(buf);
    } while (read === chunkSize);
    connectionContext.socket.send(buffer.buffer.slice(0, headerSize));
  }

  private onMessage(connectionContext: ConnectionContext) {
    let onInitResolve: any = null, onInitReject: any = null;

    connectionContext.initPromise = new Promise((resolve, reject) => {
      onInitResolve = resolve;
      onInitReject = reject;
    });

    return (message: any) => {
      const parsedMessage = this.parseMessage(message);
      const opId = parsedMessage.id;
      switch (parsedMessage.type) {
        case MessageType.GQL_CONNECTION_INIT:
          let onConnectPromise = Promise.resolve(true);
          if (this.onConnect) {
            onConnectPromise = new Promise((resolve, reject) => {
              try {
                // TODO - this should become a function call with just 2 arguments in the future
                // when we release the breaking change api: parsedMessage.payload and connectionContext
                resolve(this.onConnect(parsedMessage.payload, connectionContext.socket, connectionContext));
              } catch (e) {
                reject(e);
              }
            });
          }

          onInitResolve(onConnectPromise);

          connectionContext.initPromise.then((result) => {
            if (result === false) {
              throw new Error('Prohibited connection!');
            }

            this.sendMessage(
              connectionContext,
              undefined,
              MessageType.GQL_CONNECTION_ACK,
              undefined,
            );

            if (this.keepAlive) {
              this.sendMessage(
                connectionContext,
                undefined,
                MessageType.GQL_CONNECTION_KEEP_ALIVE,
                undefined,
              );
            }
          }).catch((error: Error) => {
            this.sendError(
              connectionContext,
              opId,
              { message: error.message },
              MessageType.GQL_CONNECTION_ERROR,
            );

            // Close the connection with an error code, ws v2 ensures that the
            // connection is cleaned up even when the closing handshake fails.
            // 1011: an unexpected condition prevented the operation from being fulfilled
            // We are using setTimeout because we want the message to be flushed before
            // disconnecting the client
            setTimeout(() => {
              connectionContext.socket.close(1011);
            }, 10);
          });
          break;

        case MessageType.GQL_CONNECTION_TERMINATE:
          connectionContext.socket.close();
          break;

        case MessageType.GQL_START:
          connectionContext.initPromise.then((initResult) => {
            // if we already have a subscription with this id, unsubscribe from it first
            if (connectionContext.operations && connectionContext.operations[opId]) {
              this.unsubscribe(connectionContext, opId);
            }
            const payload: QueryPayload = (<QueryPayload>parsedMessage.payload);

            const baseParams: ExecutionParams = {
              query: payload.query,
              variables: payload.variables,
              operationName: payload.operationName,
              context: Object.assign(
                {},
                isObject(initResult) ? initResult : {},
              ),
              formatResponse: <any>undefined,
              formatError: <any>undefined,
              callback: <any>undefined,
            };
            let promisedParams = Promise.resolve(baseParams);

            // set an initial mock subscription to only registering opId
            connectionContext.operations[opId] = createEmptyIterable();
            connectionContext.filesIn[opId] = [];
            connectionContext.filesOut[opId] = [];

            if (this.onOperation) {
              let messageForCallback: any = parsedMessage;
              promisedParams = Promise.resolve(this.onOperation(messageForCallback, baseParams, connectionContext.socket));
            }

            promisedParams.then((params: any) => {
              if (typeof params !== 'object') {
                const error = `Invalid params returned from onOperation! return values must be an object!`;
                this.sendError(connectionContext, opId, { message: error });

                throw new Error(error);
              }

              const document = typeof baseParams.query !== 'string' ? baseParams.query : parse(baseParams.query);
              let executionIterable: Promise<AsyncIterator<ExecutionResult> | ExecutionResult>;
              const validationErrors: Error[] = validate(this.schema, document, this.specifiedRules);

              if ( validationErrors.length > 0 ) {
                executionIterable = Promise.resolve(createIterableFromPromise<ExecutionResult>(
                  Promise.resolve({ errors: validationErrors }),
                ));
              } else {
                let executor: SubscribeFunction | ExecuteFunction = this.execute;
                if (this.subscribe && isASubscriptionOperation(document, params.operationName)) {
                  executor = this.subscribe;
                }

                this.processFiles(connectionContext, opId, params.variables);

                const promiseOrIterable = executor(this.schema,
                  document,
                  this.rootValue,
                  params.context,
                  params.variables,
                  params.operationName);

                if (!isAsyncIterable(promiseOrIterable) && promiseOrIterable instanceof Promise) {
                  executionIterable = promiseOrIterable;
                } else if (isAsyncIterable(promiseOrIterable)) {
                  executionIterable = Promise.resolve(promiseOrIterable as any as AsyncIterator<ExecutionResult>);
                } else {
                  // Unexpected return value from execute - log it as error and trigger an error to client side
                  console.error('Invalid `execute` return type! Only Promise or AsyncIterable are valid values!');

                  this.sendError(connectionContext, opId, {
                    message: 'GraphQL execute engine is not available',
                  });
                }
              }

              return executionIterable.then((ei) => ({
                executionIterable: isAsyncIterable(ei) ?
                  ei : createAsyncIterator([ ei ]),
                params,
              }));
            }).then(({ executionIterable, params }) => {
              forAwaitEach(
                createAsyncIterator(executionIterable) as any,
                async (value: ExecutionResult) => {
                  let result = value;
                  connectionContext.filesOut[opId] = await this.extractFiles(result.data);

                  if (params.formatResponse) {
                    try {
                      result = params.formatResponse(value, params);
                    } catch (err) {
                      console.error('Error in formatError function:', err);
                    }
                  }

                  this.sendMessage(connectionContext, opId, MessageType.GQL_DATA, result);
                }).then(async () => {
                  // Wait for all outgoing files being processed
                  while (connectionContext.filesOut[opId].length > 0) {
                    await new Promise(resolve => connectionContext.filesOutEvent.once(opId.toString(), resolve));
                  }
                })
                .then(() => {
                  this.sendMessage(connectionContext, opId, MessageType.GQL_COMPLETE, null);
                  this.unsubscribe(connectionContext, opId);
                })
                .catch((e: Error) => {
                  let error = e;

                  if (params.formatError) {
                    try {
                      error = params.formatError(e, params);
                    } catch (err) {
                      console.error('Error in formatError function: ', err);
                    }
                  }

                  // plain Error object cannot be JSON stringified.
                  if (Object.keys(e).length === 0) {
                    error = { name: e.name, message: e.message };
                  }

                  this.sendError(connectionContext, opId, error);
                });

              return executionIterable;
            }).then((subscription: ExecutionIterator) => {
              connectionContext.operations[opId] = subscription;
            }).then(() => {
              // NOTE: This is a temporary code to support the legacy protocol.
              // As soon as the old protocol has been removed, this coode should also be removed.
       //       this.sendMessage(connectionContext, opId, MessageTypes.SUBSCRIPTION_SUCCESS, undefined);
            }).catch((e: any) => {
              if (e.errors) {
                this.sendMessage(connectionContext, opId, MessageType.GQL_DATA, { errors: e.errors });
              } else {
                this.sendError(connectionContext, opId, { message: e.message });
              }

              // Remove the operation on the server side as it will be removed also in the client
              this.unsubscribe(connectionContext, opId);
              return;
            });
          });
          break;

        case MessageType.GQL_BINARY:
          connectionContext.initPromise.then(() => {
            const payload: FilePayload = (<FilePayload>parsedMessage.payload);
            if (connectionContext.filesIn[opId]) {
              const file = connectionContext.filesIn[opId].find(f => f.binary.getId() === payload.fileId);
              if (file) {
                file.reader.next(payload.buffer);
                if (payload.buffer.byteLength === 0) {
                  file.reader.complete();
                }
              }
            }
          });
          break;

        case MessageType.GQL_BINARY_REQUEST:
          connectionContext.initPromise.then(async () => {
            const payload: FileRequestPayload = (<FileRequestPayload> parsedMessage.payload);
            if (connectionContext.filesOut[opId]) {
              const { id, offset } = payload;
              const file = connectionContext.filesOut[opId].find(f => f.getId() === id);
              if (file) {
                await this.sendSingleFile(connectionContext, opId, file, offset);
                const index = connectionContext.filesOut[opId].findIndex(f => f === file);
                delete connectionContext.filesOut[opId][index];
                connectionContext.filesOutEvent.emit(opId.toString());
              }
            }
          });
          break;

        case MessageType.GQL_STOP:
          connectionContext.initPromise.then(() => {
            // Find subscription id. Call unsubscribe.
            this.unsubscribe(connectionContext, opId);
          });
          break;

        default:
          this.sendError(connectionContext, opId, { message: 'Invalid message type!' });
      }
    };
  }

  private buildMessage(id: number, type: number, payload: any): ArrayBuffer {
    let serializedMessage: string = JSON.stringify(payload) || '';

    /*
    const Message = StructType({
      id: ref.types.uint32,
      type: ref.types.uint32,
      payload: ref.types.CString,
    });
     */
    const headerSize = 4 * 2;

    const message = new DataView(new ArrayBuffer(headerSize + serializedMessage.length));
    message.setUint32(0, id, true);
    message.setUint32(4, type, true);
    new Uint8Array(message.buffer).set(Buffer.from(serializedMessage), 8);
    return message.buffer;
  }

  private sendMessage(connectionContext: ConnectionContext, opId: number, type: number, payload: any): void {
    const message = this.buildMessage(opId, type, payload);

    if (connectionContext.socket.readyState === WebSocket.OPEN) {
      connectionContext.socket.send(message);
    }
  }

  private sendError(connectionContext: ConnectionContext, opId: number, errorPayload: any,
                    overrideDefaultErrorType?: number): void {
    const sanitizedOverrideDefaultErrorType = overrideDefaultErrorType || MessageType.GQL_ERROR;
    if ([
        MessageType.GQL_CONNECTION_ERROR,
        MessageType.GQL_ERROR,
      ].indexOf(sanitizedOverrideDefaultErrorType) === -1) {
      throw new Error('overrideDefaultErrorType should be one of the allowed error messages' +
        ' GQL_CONNECTION_ERROR or GQL_ERROR');
    }

    this.sendMessage(
      connectionContext,
      opId,
      sanitizedOverrideDefaultErrorType,
      errorPayload,
    );
  }
}
