package zio.postgres
package replication

import java.time.Instant

import scala.compiletime.*
import scala.deriving.Mirror

import zio.*

import decode.{Decoder => GDecoder}
import decode.DecodeError
import decode.Field
import protocol.Packet
import protocol.Protocol

object Decoder {
  import Wal.LogicalReplication.*

  trait WalGDecoder[A, K] extends GDecoder[A, Packet, K] {
    // TODO: perhaps we should terminate on some errors and maybe some other conditions?
    override def isDone(p: Packet) = false
  }

  def message[A: TupleDecoder, K: TupleDecoder](proto: Protocol): GDecoder[Any, Packet, Wal.LogicalReplication[A, K]] =
    new WalGDecoder[Any, Wal.LogicalReplication[A, K]] {
      override def decode(s: Option[Any], p: Packet) = p match {
        case Packet.CopyData(data) =>
          ZIO.fromEither(Wal.Message.parse[A, K](data)).flatMap {
            case Wal.Message.XLogData(_, _, _, lr) =>
              ZIO.succeed(s -> Some(lr))

            case Wal.Message.PrimaryKeepAlive(walEnd, _, needReply) =>
              proto.standbyStatusUpdate(walEnd, walEnd, walEnd, Instant.now()).as(s -> None)
          }
        case _ => ZIO.succeed(s -> None)
      }

    }

  def wal[A: TupleDecoder, K: TupleDecoder]: GDecoder[Any, Packet, Wal.Message[A, K]] =
    new WalGDecoder[Any, Wal.Message[A, K]] {
      override def decode(s: Option[Any], p: Packet) = p match {
        case Packet.CopyData(data) =>
          ZIO.fromEither(Wal.Message.parse[A, K](data).map(x => s -> Option(x)))

        case _ => ZIO.succeed(s -> None)
      }

    }

  extension [A](fd: Field[A]) {

    def decode(c: Column): Either[DecodeError, A] = c match {
      case Column.Text(s)   => fd.sDecode(Some(s))
      case Column.Binary(b) => fd.bDecode(Some(b))
      case Column.NullValue => fd.sDecode(None)
      case Column.Unchanged => Left(DecodeError.Unexpected(s"Unchanged values decoding not supported"))
    }

    def single: TupleDecoder[A] = TupleDecoder {
      case a :: _ => fd.decode(a)
      case _      => Left(DecodeError.ResultSetExhausted)
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

    given TupleDecoder[EmptyTuple] = new {
      override def decode(xs: TupleData): Either[DecodeError, EmptyTuple] = Right(EmptyTuple)
    }

    given qq[T <: NonEmptyTuple](using
        hd: Field[Tuple.Head[T]],
        td: TupleDecoder[Tuple.Tail[T]]
    ): TupleDecoder[T] = new {
      override def decode(xs: TupleData): Either[DecodeError, T] = xs match {
        case h :: tl =>
          hd.decode(h).flatMap { hr =>
            td.decode(tl).map(tr => (hr *: tr).asInstanceOf[T])
          }
        case _ => Left(DecodeError.ResultSetExhausted)
      }
    }

    def forAllTableColumns[T <: Product](using
        mm: Mirror.ProductOf[T],
        td: TupleDecoder[mm.MirroredElemTypes]
    ): TupleDecoder[T] = new {
      override def decode(xs: TupleData): Either[DecodeError, T] =
        td.decode(xs).map(x => mm.fromProduct(x))
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
