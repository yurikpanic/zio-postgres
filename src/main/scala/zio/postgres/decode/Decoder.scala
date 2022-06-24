package zio.postgres
package decode

import java.nio.charset.StandardCharsets.UTF_8

import scala.util.chaining.*

import zio.*

import protocol.Packet

trait Decoder[S, F, A] {
  def decode(s: Option[S], p: F): IO[DecodeError, (Option[S], Option[A])]
  def isDone(p: F): Boolean
}

object Decoder {

  given Decoder[Packet.RowDescription, Packet, Packet.DataRow] =
    new Decoder[Packet.RowDescription, Packet, Packet.DataRow] {
      override def decode(s: Option[Packet.RowDescription], p: Packet) = p match {
        case dr: Packet.DataRow        => ZIO.succeed(s -> Some(dr))
        case rd: Packet.RowDescription => ZIO.succeed(Some(rd) -> None)
        case _                         => ZIO.succeed(s -> None)
      }

      override def isDone(p: Packet) = p match {
        case Packet.CommandComplete(_) => true
        case _                         => false
      }
    }

  extension [S <: Packet.RowDescription, F <: Packet, A <: Tuple](d: Decoder[S, F, A]) {

    def ~[B](fd: Field[B]): Decoder[S, F, Tuple.Concat[A, Tuple1[B]]] =
      new Decoder {
        override def isDone(p: F) = d.isDone(p)

        override def decode(s: Option[S], p: F) =
          d.decode(s, p).flatMap {
            case (s @ Some(rd), Some(a)) =>
              p match {
                case Packet.DataRow(fields) =>
                  fields.drop(a.size).headOption zip rd.fields.drop(a.size).headOption match {
                    case Some(x, xf) => ZIO.fromEither(fd.decode(x, xf.format).map { b => s -> Some(a ++ Tuple1(b)) })
                    case None        => ZIO.fail(DecodeError.ResultSetExhausted)
                  }
                case _ => ZIO.succeed(s -> None)
              }

            case (s @ Some(_), None) => ZIO.succeed(s -> None)

            case (None, _) => ZIO.fail(DecodeError.NoRowDescription)
          }

      }

  }
}
