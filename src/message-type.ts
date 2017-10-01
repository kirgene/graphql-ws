export enum MessageType {
  GQL_CONNECTION_INIT = 1, // Client -> Server
  GQL_CONNECTION_ACK, // Server -> Client
  GQL_CONNECTION_ERROR, // Server -> Client

  // NOTE: This one here don't follow the standard due to connection optimization
  GQL_CONNECTION_KEEP_ALIVE, // Server -> Client
  GQL_CONNECTION_TERMINATE, // Client -> Server
  GQL_START, // Client -> Server
  GQL_DATA, // Server <-> Client
  GQL_ERROR, // Server -> Client
  GQL_COMPLETE, // Server -> Client
  GQL_STOP, // Client -> Server
}
