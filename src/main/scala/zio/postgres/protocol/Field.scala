package zio.postgres.protocol

enum Field:
  case Length
  case Type(value: scala.Byte)
  case Int32(value: Int)
  case Int32s(value: Seq[Int])
  case Int16(value: Short)
  case Byte(value: scala.Byte)
  case Bytes(value: Seq[scala.Byte])
  case String(value: java.lang.String)
