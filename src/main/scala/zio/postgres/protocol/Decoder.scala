package zio.postgres.protocol

import java.nio.charset.StandardCharsets.UTF_8
import scala.util.chaining.*

trait Decoder[A] {
  def decode: PartialFunction[Packet, Either[Decoder.Error, A]]
  def isDone(p: Packet): Boolean
}

object Decoder {

  enum Error {
    case ResultSetExhausted
    case NullUnexpected
    case WalBufferUnderflow
    case UnknownWalMessage(code: Byte)
    case UnknownLogicalReplicationMessage(code: Byte)
    case UnknownLogicalReplicationUpdateKind(code: Byte)
    case Unexpected(message: String)
  }

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

    def single(using dr: Decoder[Packet.DataRow]): Decoder[A] = new Decoder {
      override def isDone(p: Packet): Boolean = dr.isDone(p)

      override def decode = dr.decode.andThen {
        case Left(err) => Left(err)
        case Right(dr) =>
          dr.fields.headOption.toRight(Error.ResultSetExhausted).flatMap(fd.decode _)
      }
    }

    def ~[B](that: Field[B])(using dr: Decoder[Packet.DataRow]): Decoder[(A, B)] = new Decoder[(A, B)] {
      override def isDone(p: Packet): Boolean = dr.isDone(p)

      override def decode =
        dr.decode.andThen {
          case Left(err) => Left(err)

          case Right(dr) =>
            dr.fields match {
              case a :: b :: _ =>
                for {
                  a <- fd.decode(a)
                  b <- that.decode(b)
                } yield a -> b
              case _ => Left(Error.ResultSetExhausted)
            }
        }
    }

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
