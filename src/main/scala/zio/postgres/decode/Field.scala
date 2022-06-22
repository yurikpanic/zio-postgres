package zio.postgres
package decode

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets.UTF_8

import zio.postgres.protocol.Packet.FieldFormat

import util.*
import protocol.Packet

trait Field[A] {
  def sDecode(data: Option[String]): Either[DecodeError, A]
  def bDecode(data: Option[Array[Byte]]): Either[DecodeError, A]

  def decode(data: Option[Array[Byte]], format: FieldFormat): Either[DecodeError, A] =
    format match {
      case FieldFormat.Textual => sDecode(data.map(new String(_, UTF_8)))
      case FieldFormat.Binary  => bDecode(data)
    }
}

object Field {

  val text: Field[String] = new Field {
    override def sDecode(data: Option[String]): Either[DecodeError, String] = data.toRight(DecodeError.NullUnexpected)
    override def bDecode(data: Option[Array[Byte]]): Either[DecodeError, String] = data
      .toRight(DecodeError.NullUnexpected)
      .map(new String(_, UTF_8))
  }

  val int: Field[Int] = new Field {
    override def sDecode(data: Option[String]): Either[DecodeError, Int] = data
      .toRight(DecodeError.NullUnexpected)
      .flatMap(_.toIntOption.toRight(DecodeError.ParseFailed(s"Failed to parse '$data' as Int")))

    override def bDecode(data: Option[Array[Byte]]): Either[DecodeError, Int] = data
      .toRight(DecodeError.NullUnexpected)
      .flatMap { arr =>
        ByteBuffer.wrap(arr).order(ByteOrder.BIG_ENDIAN).getIntSafe.toRight(DecodeError.WalBufferUnderflow)
      }
  }

  def makeOpt[A]: Either[DecodeError, A] => Either[DecodeError, Option[A]] = {
    case Left(DecodeError.NullUnexpected) => Right(None)
    case Right(x)                         => Right(Some(x))
    case Left(err)                        => Left(err)
  }

  extension [A](fd: Field[A]) {
    def opt: Field[Option[A]] = new Field {
      override def bDecode(data: Option[Array[Byte]]): Either[DecodeError, Option[A]] =
        (fd.bDecode _).andThen(makeOpt)(data)
      override def sDecode(data: Option[String]): Either[DecodeError, Option[A]] = (fd.sDecode _).andThen(makeOpt)(data)
    }

    def single(using dr: Decoder[Packet.RowDescription, Packet.DataRow]): Decoder[Packet.RowDescription, A] =
      new Decoder {
        override def isDone(p: Packet): Boolean = dr.isDone(p)

        override def decode = dr.decode.andThen {
          case Left(err) => Left(err)
          case Right(s @ Some(rd), Some(dr)) =>
            (dr.fields.headOption zip rd.fields.headOption.map(_.format))
              .toRight(DecodeError.ResultSetExhausted)
              .flatMap((fd.decode _).tupled)
              .map(x => s -> Some(x))
          case Right(s @ Some(_), None) => Right(s -> None)
          case Right(None, _)           => Left(DecodeError.NoRowDescription)
        }
      }

    def ~[B](
        that: Field[B]
    )(using dr: Decoder[Packet.RowDescription, Packet.DataRow]): Decoder[Packet.RowDescription, (A, B)] =
      new Decoder[Packet.RowDescription, (A, B)] {
        override def isDone(p: Packet): Boolean = dr.isDone(p)

        override def decode =
          dr.decode.andThen {
            case Left(err) => Left(err)

            case Right(s @ Some(rd), Some(dr)) =>
              dr.fields zip rd.fields match {
                case (a, af) :: (b, bf) :: _ =>
                  for {
                    a <- fd.decode(a, af.format)
                    b <- that.decode(b, bf.format)
                  } yield s -> Some(a -> b)

                case _ => Left(DecodeError.ResultSetExhausted)
              }

            case Right(s @ Some(_), None) => Right(s -> None)

            case Right(None, _) => Left(DecodeError.NoRowDescription)
          }
      }

  }
}
