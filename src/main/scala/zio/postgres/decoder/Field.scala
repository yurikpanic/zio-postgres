package zio.postgres
package decoder

import zio.postgres.protocol.Packet.FieldFormat

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets.UTF_8

import util.*
import protocol.Packet

trait Field[A] {
  def sDecode(data: Option[String]): Either[Error, A]
  def bDecode(data: Option[Array[Byte]]): Either[Error, A]

  def decode(data: Option[Array[Byte]], format: FieldFormat): Either[Error, A] =
    format match {
      case FieldFormat.Textual => sDecode(data.map(new String(_, UTF_8)))
      case FieldFormat.Binary  => bDecode(data)
    }
}

object Field {

  val text: Field[String] = new Field {
    override def sDecode(data: Option[String]): Either[Error, String] = data.toRight(Error.NullUnexpected)
    override def bDecode(data: Option[Array[Byte]]): Either[Error, String] = data
      .toRight(Error.NullUnexpected)
      .map(new String(_, UTF_8))
  }

  val int: Field[Int] = new Field {
    override def sDecode(data: Option[String]): Either[Error, Int] = data
      .toRight(Error.NullUnexpected)
      .flatMap(_.toIntOption.toRight(decoder.Error.ParseFailed(s"Failed to parse '$data' as Int")))

    override def bDecode(data: Option[Array[Byte]]): Either[Error, Int] = data
      .toRight(Error.NullUnexpected)
      .flatMap { arr =>
        ByteBuffer.wrap(arr).order(ByteOrder.BIG_ENDIAN).getIntSafe.toRight(Error.WalBufferUnderflow)
      }
  }

  def makeOpt[A]: Either[Error, A] => Either[Error, Option[A]] = {
    case Left(Error.NullUnexpected) => Right(None)
    case Right(x)                   => Right(Some(x))
    case Left(err)                  => Left(err)
  }

  extension [A](fd: Field[A]) {
    def opt: Field[Option[A]] = new Field {
      override def bDecode(data: Option[Array[Byte]]): Either[Error, Option[A]] = (fd.bDecode _).andThen(makeOpt)(data)
      override def sDecode(data: Option[String]): Either[Error, Option[A]] = (fd.sDecode _).andThen(makeOpt)(data)
    }

    def single(using dr: Decoder[Packet.RowDescription, Packet.DataRow]): Decoder[Packet.RowDescription, A] =
      new Decoder {
        override def isDone(p: Packet): Boolean = dr.isDone(p)

        override def decode = dr.decode.andThen {
          case Left(err) => Left(err)
          case Right(s @ Some(rd), Some(dr)) =>
            (dr.fields.headOption zip rd.fields.headOption.map(_.format))
              .toRight(Error.ResultSetExhausted)
              .flatMap((fd.decode _).tupled)
              .map(x => s -> Some(x))
          case Right(None, _) => Left(Error.NoRowDescription)
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

                case _ => Left(Error.ResultSetExhausted)
              }

            case Right(None, _) => Left(Error.NoRowDescription)
          }
      }

  }
}
