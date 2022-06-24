package zio.postgres
package decode

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets.UTF_8

import zio.*
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

  val unit: Field[Unit] = new Field {
    override def bDecode(data: Option[Array[Byte]]): Either[DecodeError, Unit] = Right(())
    override def sDecode(data: Option[String]): Either[DecodeError, Unit] = Right(())
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

    def single(using
        dr: Decoder[Packet.RowDescription, Packet, Packet.DataRow]
    ): Decoder[Packet.RowDescription, Packet, A] =
      new Decoder {
        override def isDone(p: Packet) = dr.isDone(p)

        override def decode(s: Option[Packet.RowDescription], p: Packet) =
          dr.decode(s, p).flatMap {
            case (s @ Some(rd), Some(dr)) =>
              ZIO
                .fromEither(
                  (dr.fields.headOption zip rd.fields.headOption.map(_.format))
                    .toRight(DecodeError.ResultSetExhausted)
                    .flatMap((fd.decode _).tupled)
                    .map(x => s -> Some(x))
                )
            case (s @ Some(_), None) => ZIO.succeed(s -> None)
            case (None, _)           => ZIO.fail(DecodeError.NoRowDescription)
          }

      }

    def ~[B](
        that: Field[B]
    )(using
        dr: Decoder[Packet.RowDescription, Packet, Packet.DataRow]
    ): Decoder[Packet.RowDescription, Packet, (A, B)] =
      new Decoder[Packet.RowDescription, Packet, (A, B)] {
        override def isDone(p: Packet) = dr.isDone(p)

        override def decode(s: Option[Packet.RowDescription], p: Packet) = dr.decode(s, p).flatMap {
          case (s @ Some(rd), Some(dr)) =>
            dr.fields zip rd.fields match {
              case (a, af) :: (b, bf) :: _ =>
                ZIO.fromEither(
                  for {
                    a <- fd.decode(a, af.format)
                    b <- that.decode(b, bf.format)
                  } yield s -> Some(a -> b)
                )

              case _ => ZIO.fail(DecodeError.ResultSetExhausted)
            }

          case (s @ Some(_), None) => ZIO.succeed(s -> None)

          case (None, _) => ZIO.fail(DecodeError.NoRowDescription)
        }

      }

  }
}
