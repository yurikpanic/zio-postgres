package zio.postgres
package decoder

import java.nio.charset.StandardCharsets.UTF_8
import scala.util.chaining.*

import protocol.*

trait Decoder[A] {
  def decode: PartialFunction[Packet, Either[Error, A]]
  def isDone(p: Packet): Boolean
}

object Decoder {

  def apply[A: Decoder]: Decoder[A] = summon[Decoder[A]]

  given Decoder[Packet.DataRow] = new Decoder[Packet.DataRow] {
    override def decode = { case dr: Packet.DataRow =>
      Right(dr)
    }
    override def isDone(p: Packet) = p match {
      case Packet.CommandComplete(_) => true
      case _                         => false
    }
  }

  trait Field[A] {
    def decode(data: Option[Array[Byte]]): Either[Error, A]
  }

  extension [A <: Tuple](d: Decoder[A]) {

    def ~[B](fd: Field[B])(using dr: Decoder[Packet.DataRow]): Decoder[Tuple.Concat[A, Tuple1[B]]] = new Decoder {
      override def isDone(p: Packet) = d.isDone(p)

      override def decode = dr.decode.andThen {
        case Left(err) => Left(err)

        case Right(data) if d.decode.isDefinedAt(data) =>
          d.decode(data).flatMap { a =>
            data.fields.drop(a.size) match {
              case x :: _ => fd.decode(x).map { b => a ++ Tuple1(b) }
              case _      => Left(Error.ResultSetExhausted)
            }
          }
      }
    }

  }
}
