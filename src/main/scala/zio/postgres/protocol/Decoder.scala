package zio.postgres.protocol

import java.nio.charset.StandardCharsets.UTF_8
import scala.util.chaining.*

trait Decoder[A] {
  def decode(data: Packet.DataRow): Either[Decoder.Error, A]
}

object Decoder {

  enum Error {
    case ResultSetExhausted
    case NullUnexpected
    case Unexpected(message: String)
  }

  def apply[A: Decoder]: Decoder[A] = summon[Decoder[A]]

  def apply[A](f: Packet.DataRow => Either[Decoder.Error, A]): Decoder[A] = new Decoder[A] {
    override def decode(data: Packet.DataRow): Either[Error, A] = f(data)
  }

  given Decoder[Packet.DataRow] = new Decoder[Packet.DataRow] {
    override def decode(data: Packet.DataRow) = Right(data)
  }

  trait Field[A] {
    def decode(data: Option[Array[Byte]]): Either[Error, A]
  }

  object Field {
    def apply[A](f: Option[Array[Byte]] => Either[Error, A]): Field[A] = new Field[A] {
      override def decode(data: Option[Array[Byte]]): Either[Error, A] = f(data)
    }
  }

  val textValue: Field[String] = Field(_.toRight(Error.NullUnexpected).map(new String(_, UTF_8)))

  extension [A](fd: Field[A]) {
    def opt: Field[Option[A]] = Field[Option[A]](fd.decode.andThen {
      case Left(Error.NullUnexpected) => Right(None)
      case Right(x)                   => Right(Some(x))
      case x                          => x.map(Some(_))
    })

    def single: Decoder[A] = Decoder(_.fields.headOption.toRight(Error.ResultSetExhausted).flatMap(fd.decode _))

    def ~[B](that: Field[B]): Decoder[(A, B)] = Decoder(_.fields match {
      case a :: b :: _ =>
        for {
          a1 <- fd.decode(a)
          b1 <- that.decode(b)
        } yield a1 -> b1
      case _ => Left(Error.ResultSetExhausted)
    })
  }

  extension [A <: Tuple](d: Decoder[A]) {

    def ~[B](fd: Field[B]): Decoder[Tuple.Concat[A, Tuple1[B]]] = new Decoder {

      override def decode(data: Packet.DataRow): Either[Error, Tuple.Concat[A, Tuple1[B]]] =
        d.decode(data).flatMap { a =>
          data.fields.drop(a.size) match {
            case x :: _ => fd.decode(x).map { b => a ++ Tuple1(b) }
            case _      => Left(Error.ResultSetExhausted)
          }
        }
    }
  }
}
