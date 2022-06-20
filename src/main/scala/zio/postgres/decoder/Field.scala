package zio.postgres
package decoder

import java.nio.charset.StandardCharsets.UTF_8

import protocol.Packet

trait Field[A] {
  def decode(data: Option[Array[Byte]]): Either[Error, A]
}

object Field {
  def apply[A](f: Option[Array[Byte]] => Either[Error, A]): Field[A] = new Field[A] {
    override def decode(data: Option[Array[Byte]]): Either[Error, A] = f(data)
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
}
