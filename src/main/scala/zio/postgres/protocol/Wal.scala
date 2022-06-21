package zio.postgres
package protocol

import zio.postgres
import zio.postgres.protocol.Wal.Decode.TDecoder

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets.UTF_8
import scala.Tuple.Concat
import scala.util.Try

import util.*
import decoder.Decoder

object Wal {
  enum Message[A] {
    case PrimaryKeepAlive(walEnd: Long, clock: Long, needReply: Boolean)
    case XLogData(startingPoint: Long, walEnd: Long, clock: Long, data: LogicalReplication[A])
  }

  // TODO: no xid field support in streamed transactions data messages
  //       do we need protocol v 2 at this stage at all?
  enum LogicalReplication[+A] {
    case Begin(finalLSN: Long, timestamp: Long, xid: Int)
    case Message(flags: Byte, lsn: Long, prefix: String, content: Array[Byte])
    case Commit(flags: Byte, lsn: Long, endLsn: Long, timestamp: Long)
    case Origin(lsn: Long, name: String)
    case Relation(
        id: Int,
        namespace: String,
        name: String,
        replicaIdentity: Byte,
        columns: List[LogicalReplication.Relation.Column]
    )
    case Type(id: Int, namespace: String, name: String)
    case Insert(relationId: Int, tuples: A)
    case Update(
        relationId: Int,
        // Old data is present only if the data in REPLICA IDENTTY (primary key by default) was changed
        // The left projection is tuples from the REPLICA IDENTITY (primary key)
        // The right projection - is full old tuple - if REPLICA IDENTITY was set to FULL
        oldTuples: Option[Either[A, A]],
        newTuples: A
    )
    case Delete(
        relationId: Int,
        oldTuples: Either[A, A]
    )
    case Truncate(
        option: Byte,
        relationIds: List[Int]
    )

    case StreamStart(xid: Int, firstSegment: Boolean)
    case StreamStop
    case StreamCommit(xid: Int, flags: Byte, lsn: Long, endLsn: Long, timestamp: Long)
    case StreamAbort(xid: Int, subTransXid: Int)
  }

  object Message {
    def parse[A: TDecoder](data: Array[Byte]): Either[decoder.Error, Message[A]] = {
      val bb = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN)

      (bb.getByteSafe.toRight(decoder.Error.WalBufferUnderflow).flatMap {
        case 'k' =>
          (for {
            walEnd <- bb.getLongSafe
            clock <- bb.getLongSafe
            needReply <- bb.getByteSafe
          } yield PrimaryKeepAlive(walEnd, clock, needReply == 1)).toRight(decoder.Error.WalBufferUnderflow)

        case 'w' =>
          (for {
            startingPoint <- bb.getLongSafe
            walEnd <- bb.getLongSafe
            clock <- bb.getLongSafe
          } yield (startingPoint, walEnd, clock)).toRight(decoder.Error.WalBufferUnderflow).flatMap {
            case (startingPoint, walEnd, clock) =>
              LogicalReplication.parse(bb).map { data => XLogData(startingPoint, walEnd, clock, data) }
          }

        case other => Left(decoder.Error.UnknownWalMessage(other))
      })
    }
  }

  object LogicalReplication {
    def parse[A: TDecoder](bb: ByteBuffer): Either[decoder.Error, LogicalReplication[A]] = {
      bb.getByteSafe.toRight(decoder.Error.WalBufferUnderflow).flatMap {
        case 'B'   => Begin.parse(bb)
        case 'M'   => Message.parse(bb)
        case 'C'   => Commit.parse(bb)
        case 'O'   => Origin.parse(bb)
        case 'R'   => Relation.parse(bb)
        case 'Y'   => Type.parse(bb)
        case 'I'   => Insert.parse(bb)
        case 'U'   => Update.parse(bb)
        case 'D'   => Delete.parse(bb)
        case 'T'   => Truncate.parse(bb)
        case 'S'   => StreamStart.parse(bb)
        case 'E'   => Right(StreamStop)
        case 'c'   => StreamCommit.parse(bb)
        case 'A'   => StreamAbort.parse(bb)
        case other => Left(decoder.Error.UnknownLogicalReplicationMessage(other))
      }
    }

    type TupleData = List[Column]

    object TupleData {

      def parse[A: TDecoder](bb: ByteBuffer): Either[decoder.Error, A] = {
        bb.getShortSafe.toRight(decoder.Error.WalBufferUnderflow).flatMap { num =>
          (0 until num)
            .foldLeft[Either[decoder.Error, TupleData]](Right(Nil)) { (acc, _) =>
              acc.flatMap { acc =>
                Column
                  .parse(bb)
                  .map(_ :: acc)
              }
            }
            .map(_.reverse)
            .flatMap(summon[TDecoder[A]].decode _)
        }
      }
    }

    enum Column {
      case NullValue
      case Unchanged
      case Text(value: String)
      case Binary(value: Array[Byte])
    }

    object Column {
      def parse(bb: ByteBuffer): Either[decoder.Error, Column] = {
        bb.getByteSafe.toRight(decoder.Error.WalBufferUnderflow).flatMap {
          case 'n' => Right(NullValue)
          case 'u' => Right(Unchanged)
          case 't' =>
            (for {
              len <- bb.getIntSafe
              arr = new Array[Byte](len)
              _ <- Try(bb.get(arr)).toOption
              value = new String(arr, UTF_8)
            } yield Text(value)).toRight(decoder.Error.WalBufferUnderflow)
          case 'b' =>
            (for {
              len <- bb.getIntSafe
              arr = new Array[Byte](len)
              _ <- Try(bb.get(arr)).toOption
            } yield Binary(arr)).toRight(decoder.Error.WalBufferUnderflow)
        }
      }

    }

    object Begin {

      def parse[A](bb: ByteBuffer): Either[decoder.Error, LogicalReplication[A]] = {
        for {
          lsn <- bb.getLongSafe
          ts <- bb.getLongSafe
          xid <- bb.getIntSafe
        } yield Begin(lsn, ts, xid)
      }.toRight(decoder.Error.WalBufferUnderflow)
    }

    object Message {
      def parse[A](bb: ByteBuffer): Either[decoder.Error, LogicalReplication[A]] = {
        for {
          flags <- bb.getByteSafe
          lsn <- bb.getLongSafe
          prefix <- bb.getString
          len <- bb.getIntSafe
          arr = new Array[Byte](len)
          _ <- Try(bb.get(arr)).toOption
        } yield Message(flags, lsn, prefix, arr)
      }.toRight(decoder.Error.WalBufferUnderflow)
    }

    object Commit {
      def parse[A](bb: ByteBuffer): Either[decoder.Error, LogicalReplication[A]] = {
        for {
          flags <- bb.getByteSafe
          lsn <- bb.getLongSafe
          endLsn <- bb.getLongSafe
          ts <- bb.getLongSafe
        } yield Commit(flags, lsn, endLsn, ts)
      }.toRight(decoder.Error.WalBufferUnderflow)
    }

    object Origin {
      def parse[A](bb: ByteBuffer): Either[decoder.Error, LogicalReplication[A]] = {
        for {
          lsn <- bb.getLongSafe
          name <- bb.getString
        } yield Origin(lsn, name)
      }.toRight(decoder.Error.WalBufferUnderflow)
    }

    object Relation {
      final case class Column(flags: Byte, name: String, dataTypeId: Int, typeModifier: Int)

      object Column {
        def parse(bb: ByteBuffer): Either[decoder.Error, Column] = {
          for {
            flags <- bb.getByteSafe
            name <- bb.getString
            dtId <- bb.getIntSafe
            typMod <- bb.getIntSafe
          } yield Column(flags, name, dtId, typMod)
        }.toRight(decoder.Error.WalBufferUnderflow)

        extension (c: Column) {
          def isKey: Boolean = (c.flags & 1) != 0
        }
      }

      def parse[A](bb: ByteBuffer): Either[decoder.Error, LogicalReplication[A]] = {
        for {
          id <- bb.getIntSafe
          ns <- bb.getString
          name <- bb.getString
          replicaIdentity <- bb.getByteSafe
          numColumns <- bb.getShortSafe
        } yield (id, ns, name, replicaIdentity, numColumns)
      }.toRight(decoder.Error.WalBufferUnderflow).flatMap { case (id, ns, name, replicaIdentity, numColumns) =>
        {
          (0 until numColumns).foldLeft[Either[decoder.Error, List[Column]]](Right(Nil)) { (acc, _) =>
            acc.flatMap { acc =>
              Column.parse(bb).map(_ :: acc)
            }
          }
        }.map(xs => Relation(id, ns, name, replicaIdentity, xs.reverse))
      }
    }

    object Type {
      def parse[A](bb: ByteBuffer): Either[decoder.Error, LogicalReplication[A]] = {
        for {
          id <- bb.getIntSafe
          ns <- bb.getString
          name <- bb.getString
        } yield Type(id, ns, name)
      }.toRight(decoder.Error.WalBufferUnderflow)
    }

    object Insert {
      def parse[A: TDecoder](bb: ByteBuffer): Either[decoder.Error, LogicalReplication[A]] = {
        (for {
          id <- bb.getIntSafe
          n <- bb.getByteSafe
        } yield id).toRight(decoder.Error.WalBufferUnderflow).flatMap { id =>
          TupleData.parse(bb).map(Insert(id, _))
        }
      }
    }

    object Update {
      def parse[A: TDecoder](bb: ByteBuffer): Either[decoder.Error, LogicalReplication[A]] = {
        (for {
          id <- bb.getIntSafe
          keyKind <- bb.getByteSafe
        } yield (id, keyKind)).toRight(decoder.Error.WalBufferUnderflow).flatMap { case (id, keyKind) =>
          TupleData.parse(bb).flatMap { data =>
            if (keyKind == 'N') Right(Update(id, oldTuples = None, newTuples = data))
            else
              bb.getByteSafe.toRight(decoder.Error.WalBufferUnderflow).flatMap { _ =>
                TupleData.parse(bb).flatMap { newData =>
                  keyKind match {
                    case 'K'   => Right(Update(id, oldTuples = Some(Left(data)), newTuples = newData))
                    case 'O'   => Right(Update(id, oldTuples = Some(Right(data)), newTuples = newData))
                    case other => Left(decoder.Error.UnknownLogicalReplicationUpdateKind(other))
                  }
                }
              }
          }
        }
      }
    }

    object Delete {
      def parse[A: TDecoder](bb: ByteBuffer): Either[decoder.Error, LogicalReplication[A]] = {
        (for {
          id <- bb.getIntSafe
          keyKind <- bb.getByteSafe
        } yield (id, keyKind)).toRight(decoder.Error.WalBufferUnderflow).flatMap { case (id, keyKind) =>
          TupleData.parse(bb).flatMap { data =>
            keyKind match {
              case 'K'   => Right(Delete(id, Left(data)))
              case 'O'   => Right(Delete(id, Right(data)))
              case other => Left(decoder.Error.UnknownLogicalReplicationUpdateKind(other))
            }
          }
        }
      }
    }

    object Truncate {
      def parse[A](bb: ByteBuffer): Either[decoder.Error, LogicalReplication[A]] = {
        (for {
          numRels <- bb.getIntSafe
          option <- bb.getByteSafe
        } yield (numRels, option)).toRight(decoder.Error.WalBufferUnderflow).flatMap { case (numRels, option) =>
          {
            (0 until numRels).foldLeft[Option[List[Int]]](Some(Nil)) { (acc, _) =>
              acc.flatMap { acc =>
                bb.getIntSafe.map(_ :: acc)
              }
            }
          }.toRight(decoder.Error.WalBufferUnderflow)
            .map(xs => Truncate(option, xs.reverse))
        }
      }
    }

    object StreamStart {
      def parse[A](bb: ByteBuffer): Either[decoder.Error, LogicalReplication[A]] = {
        for {
          xid <- bb.getIntSafe
          firstSegment <- bb.getByteSafe
        } yield StreamStart(xid, firstSegment == 1)
      }.toRight(decoder.Error.WalBufferUnderflow)
    }

    object StreamCommit {
      def parse[A](bb: ByteBuffer): Either[decoder.Error, LogicalReplication[A]] = {
        for {
          xid <- bb.getIntSafe
          flags <- bb.getByteSafe
          lsn <- bb.getLongSafe
          endLsn <- bb.getLongSafe
          ts <- bb.getLongSafe
        } yield StreamCommit(xid, flags, lsn, endLsn, ts)
      }.toRight(decoder.Error.WalBufferUnderflow)
    }

    object StreamAbort {
      def parse[A](bb: ByteBuffer): Either[decoder.Error, LogicalReplication[A]] = {
        for {
          xid <- bb.getIntSafe
          subXid <- bb.getIntSafe
        } yield StreamAbort(xid, subXid)
      }.toRight(decoder.Error.WalBufferUnderflow)
    }
  }

  object Decode {
    import LogicalReplication.*

    def apply[A: TDecoder]: Decoder[_, Wal.Message[A]] =
      new Decoder[_, Wal.Message[A]] {
        override def decode = { case (s, Packet.CopyData(data)) =>
          Wal.Message.parse(data).map(x => s -> Some(x))
        }

        // TODO: perhaps we should terminate on some errors and maybe some other conditions?
        override def isDone(p: Packet): Boolean = false
      }

    extension [A](fd: decoder.Field[A]) {

      def decode(c: Column): Either[decoder.Error, A] = c match {
        case Column.Text(s)   => fd.sDecode(Some(s))
        case Column.Binary(b) => fd.bDecode(Some(b))
        case Column.NullValue => fd.sDecode(None)
        case Column.Unchanged => Left(decoder.Error.Unexpected(s"Unchanged values decoding not supported"))
      }

      def ~[B](that: decoder.Field[B]): TDecoder[(A, B)] = new TDecoder {
        override def decode(xs: TupleData) = xs match {
          case a :: b :: _ => fd.decode(a).flatMap { a => that.decode(b).map(a -> _) }
          case _           => Left(decoder.Error.ResultSetExhausted)
        }
      }
    }

    trait TDecoder[A] {
      def decode(xs: TupleData): Either[decoder.Error, A]
    }

    object TDecoder {
      def apply[A](fn: TupleData => Either[decoder.Error, A]): TDecoder[A] = new TDecoder[A] {
        override def decode(xs: TupleData): Either[decoder.Error, A] = fn(xs)
      }

      def apply[A](fd: decoder.Field[A]): TDecoder[A] = new TDecoder[A] {
        override def decode(xs: TupleData): Either[decoder.Error, A] = xs match {
          case c :: _ => fd.decode(c)
          case _      => Left(decoder.Error.ResultSetExhausted)
        }
      }

      given TDecoder[TupleData] = TDecoder((identity[TupleData] _).andThen(Right(_)))

      extension [A <: Tuple](self: TDecoder[A]) {
        def ~[B](fd: decoder.Field[B]): TDecoder[Tuple.Concat[A, Tuple1[B]]] = new TDecoder {
          override def decode(xs: TupleData): Either[decoder.Error, Concat[A, Tuple1[B]]] =
            self.decode(xs).flatMap { a =>
              (xs.drop(a.size) match {
                case c :: _ => fd.decode(c)
                case _      => Left(decoder.Error.ResultSetExhausted)
              }).map { b => a ++ Tuple1(b) }
            }
        }
      }

    }
  }

  def standbyStatusUpdate(
      walWritten: Long,
      walFlushed: Long,
      walApplied: Long,
      clock: Long,
      replyNow: Boolean = false
  ): Array[Byte] = {
    val _replyNow: Byte = if (replyNow) 1 else 0
    val bb = Gen.make(
      Field.Byte('r'),
      Field.Int64(walWritten),
      Field.Int64(walFlushed),
      Field.Int64(walApplied),
      Field.Int64(clock),
      Field.Byte(_replyNow)
    )

    val arr = new Array[Byte](bb.limit() - bb.position())
    bb.get(arr)
    arr
  }
}
