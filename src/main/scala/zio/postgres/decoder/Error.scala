package zio.postgres.decoder

enum Error {
  case ResultSetExhausted
  case NoRowDescription
  case NullUnexpected
  case WalBufferUnderflow
  case UnknownWalMessage(code: Byte)
  case UnknownLogicalReplicationMessage(code: Byte)
  case UnknownLogicalReplicationUpdateKind(code: Byte)
  case ParseFailed(message: String)
  case Unexpected(message: String)
}
