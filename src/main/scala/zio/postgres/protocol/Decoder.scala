package zio.postgres.protocol

import zio.postgres.protocol.Packet.DataRow

trait Decoder[A] {
  def decode(data: Packet.DataRow): Either[Decoder.Error, A]
}

object Decoder {

  enum Error {
    case Unexpected(message: String)
  }

  def apply[A: Decoder]: Decoder[A] = summon[Decoder[A]]

  def apply[A](f: Packet.DataRow => Either[Decoder.Error, A]): Decoder[A] = new Decoder[A] {
    override def decode(data: Packet.DataRow): Either[Error, A] = f(data)
  }

  given Decoder[Packet.DataRow] = new Decoder[Packet.DataRow] {
    override def decode(data: DataRow) = Right(data)
  }
}
