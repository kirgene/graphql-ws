import * as WebSocket from 'uws';
import { EventEmitter } from 'events';
import * as jiff from 'jiff';

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
import {Binary} from './Binary';
import {
  FileRequestPayload, extractIncomingFiles, extractOutgoingFiles, buildMessage, parseMessage, SocketWrapper,
} from './common';
import {BinarySender} from './BinarySender';

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
  socket: SocketWrapper,
  filesIn: {
    [id: number]: Binary[],
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
  payload?: QueryPayload | FileRequestPayload;
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
  formatError?: Function;
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
  private formatError: Function;
  private rootValue: any;
  private keepAlive: number;
  private closeHandler: () => void;
  private specifiedRules: Array<(context: ValidationContext) => any>;

  public static create(options: ServerOptions, socketOptions: WebSocket.IServerOptions) {
    return new SubscriptionServer(options, socketOptions);
  }

  private static sendMessage(connectionContext: ConnectionContext, opId: number, type: number, payload: any): void {
    const message = buildMessage(opId, type, payload);

    if (connectionContext.socket.readyState === WebSocket.OPEN) {
      connectionContext.socket.send(message);
    }
  }

  private static sendError(connectionContext: ConnectionContext, opId: number, errorPayload: any,
                           overrideDefaultErrorType?: number): void {
    const sanitizedOverrideDefaultErrorType = overrideDefaultErrorType || MessageType.GQL_ERROR;
    if ([
        MessageType.GQL_CONNECTION_ERROR,
        MessageType.GQL_ERROR,
      ].indexOf(sanitizedOverrideDefaultErrorType) === -1) {
      throw new Error('overrideDefaultErrorType should be one of the allowed error messages' +
        ' GQL_CONNECTION_ERROR or GQL_ERROR');
    }

    SubscriptionServer.sendMessage(
      connectionContext,
      opId,
      sanitizedOverrideDefaultErrorType,
      errorPayload,
    );
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

    const connectionHandler = ((socket: WebSocket) => {
      if (socket.upgradeReq.headers['sec-websocket-protocol'] !== GRAPHQL_WS) {
        // Close the connection with an error code, ws v2 ensures that the
        // connection is cleaned up even when the closing handshake fails.
        // 1002: protocol error
        socket.close(1002);
        return;
      }

      const connectionContext: ConnectionContext = Object.create(null);
      connectionContext.socket = new SocketWrapper(socket);
      connectionContext.operations = {};
      connectionContext.filesIn = {};
      connectionContext.filesOut = {};
      connectionContext.filesOutEvent = new EventEmitter();

      // Regular keep alive messages if keepAlive is set
      if (this.keepAlive) {
        const keepAliveTimer = setInterval(() => {
          if (socket.readyState === WebSocket.OPEN) {
            SubscriptionServer.sendMessage(connectionContext, undefined, MessageType.GQL_CONNECTION_KEEP_ALIVE, undefined);
          } else {
            clearInterval(keepAliveTimer);
          }
        }, this.keepAlive);
      }

      const connectionClosedHandler = (error: any) => {
        if (error) {
          SubscriptionServer.sendError(
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
      connectionContext.socket.on('message', this.onMessage(connectionContext));
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
    const { execute, subscribe, schema, rootValue, formatError } = options;

    if (!execute) {
      throw new Error('Must provide `execute` for websocket server constructor.');
    }

    if (!schema) {
      throw new Error('`schema` is missing');
    }

//    Object.assign(schema.getTypeMap(), File);

    this.schema = schema;
    this.rootValue = rootValue;
    this.formatError = formatError;
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

  private onMessage(connectionContext: ConnectionContext) {
    let onInitResolve: any = null, onInitReject: any = null;

    connectionContext.initPromise = new Promise((resolve, reject) => {
      onInitResolve = resolve;
      onInitReject = reject;
    });

    return (message: any) => {
      const parsedMessage = parseMessage(message);
      if (!parsedMessage) {
        return;
      }
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

            SubscriptionServer.sendMessage(
              connectionContext,
              undefined,
              MessageType.GQL_CONNECTION_ACK,
              undefined,
            );

            if (this.keepAlive) {
              SubscriptionServer.sendMessage(
                connectionContext,
                undefined,
                MessageType.GQL_CONNECTION_KEEP_ALIVE,
                undefined,
              );
            }
          }).catch((error: Error) => {
            SubscriptionServer.sendError(
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
                SubscriptionServer.sendError(connectionContext, opId, { message: error });

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

                connectionContext.filesIn[opId] = extractIncomingFiles(opId, connectionContext.socket, params.variables);

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

                  SubscriptionServer.sendError(connectionContext, opId, {
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
              let lastVal: ExecutionResult;
              forAwaitEach(
                createAsyncIterator(executionIterable) as any,
                (value: ExecutionResult) => {
                  let result = value;
                  connectionContext.filesOut[opId] = extractOutgoingFiles(result.data);

                  if (params.formatResponse) {
                    try {
                      result = params.formatResponse(value, params);
                    } catch (err) {
                      console.error('Error in formatError function:', err);
                    }
                  }
                  if (lastVal) {
                    result = { data: { __patch: jiff.diff(lastVal.data, value.data, { invertible : false } ) } };
                  } else {
                    result = value;
                  }
                  lastVal = value;
                  SubscriptionServer.sendMessage(connectionContext, opId, MessageType.GQL_DATA, result);
                }).then(async () => {
                  // Wait for all outgoing files being processed
                  while (connectionContext.filesOut[opId] && connectionContext.filesOut[opId].length > 0) {
                    await new Promise(resolve => connectionContext.filesOutEvent.once(opId.toString(), resolve));
                  }
                })
                .then(() => {
                  SubscriptionServer.sendMessage(connectionContext, opId, MessageType.GQL_COMPLETE, null);
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

                  SubscriptionServer.sendError(connectionContext, opId, error);
                });

              return executionIterable;
            }).then((subscription: ExecutionIterator) => {
              connectionContext.operations[opId] = subscription;
            }).then(() => {
              // NOTE: This is a temporary code to support the legacy protocol.
              // As soon as the old protocol has been removed, this coode should also be removed.
       //       SubscriptionServer.sendMessage(connectionContext, opId, MessageTypes.SUBSCRIPTION_SUCCESS, undefined);
            }).catch((e: any) => {
              if (e.errors) {
                SubscriptionServer.sendMessage(connectionContext, opId, MessageType.GQL_DATA, { errors: e.errors });
              } else {
                SubscriptionServer.sendError(connectionContext, opId, { message: e.message });
              }

              // Remove the operation on the server side as it will be removed also in the client
              this.unsubscribe(connectionContext, opId);
              return;
            });
          });
          break;

        case MessageType.GQL_BINARY_REQUEST:
          connectionContext.initPromise.then(async () => {
            const payload: FileRequestPayload = (<FileRequestPayload> parsedMessage.payload);
            if (connectionContext.filesOut[opId]) {
              const { id, offset } = payload;
              const file = connectionContext.filesOut[opId].find(f => f.getId() === id);
              if (file) {
                const reader = file.createReadStream(offset);
                const writer = new BinarySender({
                  opId,
                  fileId: file.getId(),
                  socket: connectionContext.socket,
                });
                const finish = new Promise((resolve, reject) => {
                  reader.pipe(writer)
                    .on('finish', resolve)
                    .on('error', reject);
                });
                await finish;
                if (connectionContext.filesOut[opId]) {
                  const index = connectionContext.filesOut[opId].findIndex(f => f === file);
                  delete connectionContext.filesOut[opId][index];
                  connectionContext.filesOutEvent.emit(opId.toString());
                }
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
          // SubscriptionServer.sendError(connectionContext, opId, { message: 'Invalid message type!' });
          break;
      }
    };
  }
}
