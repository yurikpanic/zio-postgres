package zio.postgres
package replication

import decode.{Decoder => GDecoder}
import decode.DecodeError
import decode.Field
import protocol.Packet

object Decoder {
  import Wal.LogicalReplication.*

  def apply[A: TupleDecoder]: GDecoder[_, Wal.Message[A]] =
    new GDecoder[_, Wal.Message[A]] {
      override def decode = { case (s, Packet.CopyData(data)) =>
        Wal.Message.parse(data).map(x => s -> Option(x))
      }

      // TODO: perhaps we should terminate on some errors and maybe some other conditions?
      override def isDone(p: Packet): Boolean = false
    }

  extension [A](fd: Field[A]) {

    def decode(c: Column): Either[DecodeError, A] = c match {
      case Column.Text(s)   => fd.sDecode(Some(s))
      case Column.Binary(b) => fd.bDecode(Some(b))
      case Column.NullValue => fd.sDecode(None)
      case Column.Unchanged => Left(DecodeError.Unexpected(s"Unchanged values decoding not supported"))
    }

    def ~[B](that: Field[B]): TupleDecoder[(A, B)] = new TupleDecoder {
      override def decode(xs: TupleData) = xs match {
        case a :: b :: _ => fd.decode(a).flatMap { a => that.decode(b).map(a -> _) }
        case _           => Left(DecodeError.ResultSetExhausted)
      }
    }
  }

  trait TupleDecoder[A] {
    def decode(xs: TupleData): Either[DecodeError, A]
  }

  object TupleDecoder {
    def apply[A](fn: TupleData => Either[DecodeError, A]): TupleDecoder[A] = new TupleDecoder[A] {
      override def decode(xs: TupleData): Either[DecodeError, A] = fn(xs)
    }

    def apply[A](fd: Field[A]): TupleDecoder[A] = new TupleDecoder[A] {
      override def decode(xs: TupleData): Either[DecodeError, A] = xs match {
        case c :: _ => fd.decode(c)
        case _      => Left(DecodeError.ResultSetExhausted)
      }
    }

    given TupleDecoder[TupleData] = TupleDecoder((identity[TupleData] _).andThen(Right(_)))

    extension [A <: Tuple](self: TupleDecoder[A]) {
      def ~[B](fd: Field[B]): TupleDecoder[Tuple.Concat[A, Tuple1[B]]] = new TupleDecoder {
        override def decode(xs: TupleData): Either[DecodeError, Tuple.Concat[A, Tuple1[B]]] =
          self.decode(xs).flatMap { a =>
            (xs.drop(a.size) match {
              case c :: _ => fd.decode(c)
              case _      => Left(DecodeError.ResultSetExhausted)
            }).map { b => a ++ Tuple1(b) }
          }
      }
    }

  }
}
