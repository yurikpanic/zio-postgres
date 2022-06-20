package zio.postgres
package decoder

import java.nio.charset.StandardCharsets.UTF_8
import scala.util.chaining.*

import protocol.*

trait Decoder[S, A] {
  def decode: PartialFunction[(Option[S], Packet), Either[Error, (Option[S], Option[A])]]
  def isDone(p: Packet): Boolean
}

object Decoder {

  given Decoder[Packet.RowDescription, Packet.DataRow] = new Decoder[Packet.RowDescription, Packet.DataRow] {
    override def decode = { case (s, dr: Packet.DataRow) =>
      Right(s -> Some(dr))
    }
    override def isDone(p: Packet) = p match {
      case Packet.CommandComplete(_) => true
      case _                         => false
    }
  }

  trait Field[A] {
    def decode(data: Option[Array[Byte]]): Either[Error, A]
  }

  extension [S, A <: Tuple](d: Decoder[S, A]) {

    def ~[B](fd: Field[B])(using dr: Decoder[S, Packet.DataRow]): Decoder[S, Tuple.Concat[A, Tuple1[B]]] = new Decoder {
      override def isDone(p: Packet) = d.isDone(p)

      override def decode = dr.decode.andThen {
        case Left(err) => Left(err)

        case Right(s, Some(data)) if d.decode.isDefinedAt(s -> data) =>
          d.decode(s -> data).flatMap {
            case (s, Some(a)) =>
              data.fields.drop(a.size) match {
                case x :: _ => fd.decode(x).map { b => s -> Some(a ++ Tuple1(b)) }
                case _      => Left(Error.ResultSetExhausted)
              }

            case (s, None) =>
              Right(s -> None)
          }

        case Right(s, None) => Right(s -> None)
      }
    }

  }
}
