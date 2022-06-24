package zio.postgres
package decode

import java.nio.charset.StandardCharsets.UTF_8

import scala.util.chaining.*

import zio.*

import protocol.*

trait Decoder[S, A] {
  def decode(s: Option[S], p: Packet): IO[DecodeError, (Option[S], Option[A])]
  def isDefinedAt(p: Packet): Boolean
  def isDone(p: Packet): Boolean
}

object Decoder {

  given Decoder[Packet.RowDescription, Packet.DataRow] = new Decoder[Packet.RowDescription, Packet.DataRow] {
    override def decode(s: Option[Packet.RowDescription], p: Packet) = p match {
      case dr: Packet.DataRow        => ZIO.succeed(s -> Some(dr))
      case rd: Packet.RowDescription => ZIO.succeed(Some(rd) -> None)
      case _                         => ZIO.succeed(s -> None)
    }

    override def isDefinedAt(p: Packet): Boolean = p match {
      case _: Packet.DataRow        => true
      case _: Packet.RowDescription => true
      case _                        => false
    }

    override def isDone(p: Packet) = p match {
      case Packet.CommandComplete(_) => true
      case _                         => false
    }
  }

  trait Field[A] {
    def decode(data: Option[Array[Byte]]): Either[DecodeError, A]
  }

  extension [S, A <: Tuple](d: Decoder[S, A]) {

    def ~[B](fd: Field[B])(using dr: Decoder[S, Packet.DataRow]): Decoder[S, Tuple.Concat[A, Tuple1[B]]] = new Decoder {
      override def isDone(p: Packet) = dr.isDone(p) || d.isDone(p)

      override def isDefinedAt(p: Packet) = dr.isDefinedAt(p) && d.isDefinedAt(p)

      override def decode(s: Option[S], p: Packet) = dr.decode(s, p).flatMap {
        case (s, Some(data)) =>
          d.decode(s, data).flatMap {
            case (s, Some(a)) =>
              data.fields.drop(a.size) match {
                case x :: _ => ZIO.fromEither(fd.decode(x).map { b => s -> Some(a ++ Tuple1(b)) })
                case _      => ZIO.fail(DecodeError.ResultSetExhausted)
              }

            case (s, None) =>
              ZIO.succeed(s -> None)
          }

        case (s, None) =>
          ZIO.succeed(s -> None)
      }

    }

  }
}
