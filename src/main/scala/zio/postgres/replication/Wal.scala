package zio.postgres
package replication

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets.UTF_8

import scala.util.Try

import decode.DecodeError
import decode.Decoder
import protocol.Packet
import replication.Decoder.TupleDecoder
import util.*

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
    def parse[A: TupleDecoder](data: Array[Byte]): Either[DecodeError, Message[A]] = {
      val bb = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN)

      (bb.getByteSafe.toRight(DecodeError.WalBufferUnderflow).flatMap {
        case 'k' =>
          (for {
            walEnd <- bb.getLongSafe
            clock <- bb.getLongSafe
            needReply <- bb.getByteSafe
          } yield PrimaryKeepAlive(walEnd, clock, needReply == 1)).toRight(DecodeError.WalBufferUnderflow)

        case 'w' =>
          (for {
            startingPoint <- bb.getLongSafe
            walEnd <- bb.getLongSafe
            clock <- bb.getLongSafe
          } yield (startingPoint, walEnd, clock)).toRight(DecodeError.WalBufferUnderflow).flatMap {
            case (startingPoint, walEnd, clock) =>
              LogicalReplication.parse(bb).map { data => XLogData(startingPoint, walEnd, clock, data) }
          }

        case other => Left(DecodeError.UnknownWalMessage(other))
      })
    }
  }

  object LogicalReplication {
    def parse[A: TupleDecoder](bb: ByteBuffer): Either[DecodeError, LogicalReplication[A]] = {
      bb.getByteSafe.toRight(DecodeError.WalBufferUnderflow).flatMap {
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
        case other => Left(DecodeError.UnknownLogicalReplicationMessage(other))
      }
    }

    type TupleData = List[Column]

    object TupleData {

      def parse[A: TupleDecoder](bb: ByteBuffer): Either[DecodeError, A] = {
        bb.getShortSafe.toRight(DecodeError.WalBufferUnderflow).flatMap { num =>
          (0 until num)
            .foldLeft[Either[DecodeError, TupleData]](Right(Nil)) { (acc, _) =>
              acc.flatMap { acc =>
                Column
                  .parse(bb)
                  .map(_ :: acc)
              }
            }
            .map(_.reverse)
            .flatMap(summon[TupleDecoder[A]].decode _)
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
      def parse(bb: ByteBuffer): Either[DecodeError, Column] = {
        bb.getByteSafe.toRight(DecodeError.WalBufferUnderflow).flatMap {
          case 'n' => Right(NullValue)
          case 'u' => Right(Unchanged)
          case 't' =>
            (for {
              len <- bb.getIntSafe
              arr = new Array[Byte](len)
              _ <- Try(bb.get(arr)).toOption
              value = new String(arr, UTF_8)
            } yield Text(value)).toRight(DecodeError.WalBufferUnderflow)
          case 'b' =>
            (for {
              len <- bb.getIntSafe
              arr = new Array[Byte](len)
              _ <- Try(bb.get(arr)).toOption
            } yield Binary(arr)).toRight(DecodeError.WalBufferUnderflow)
        }
      }

    }

    object Begin {

      def parse[A](bb: ByteBuffer): Either[DecodeError, LogicalReplication[A]] = {
        for {
          lsn <- bb.getLongSafe
          ts <- bb.getLongSafe
          xid <- bb.getIntSafe
        } yield Begin(lsn, ts, xid)
      }.toRight(DecodeError.WalBufferUnderflow)
    }

    object Message {
      def parse[A](bb: ByteBuffer): Either[DecodeError, LogicalReplication[A]] = {
        for {
          flags <- bb.getByteSafe
          lsn <- bb.getLongSafe
          prefix <- bb.getString
          len <- bb.getIntSafe
          arr = new Array[Byte](len)
          _ <- Try(bb.get(arr)).toOption
        } yield Message(flags, lsn, prefix, arr)
      }.toRight(DecodeError.WalBufferUnderflow)
    }

    object Commit {
      def parse[A](bb: ByteBuffer): Either[DecodeError, LogicalReplication[A]] = {
        for {
          flags <- bb.getByteSafe
          lsn <- bb.getLongSafe
          endLsn <- bb.getLongSafe
          ts <- bb.getLongSafe
        } yield Commit(flags, lsn, endLsn, ts)
      }.toRight(DecodeError.WalBufferUnderflow)
    }

    object Origin {
      def parse[A](bb: ByteBuffer): Either[DecodeError, LogicalReplication[A]] = {
        for {
          lsn <- bb.getLongSafe
          name <- bb.getString
        } yield Origin(lsn, name)
      }.toRight(DecodeError.WalBufferUnderflow)
    }

    object Relation {
      final case class Column(flags: Byte, name: String, dataTypeId: Int, typeModifier: Int)

      object Column {
        def parse(bb: ByteBuffer): Either[DecodeError, Column] = {
          for {
            flags <- bb.getByteSafe
            name <- bb.getString
            dtId <- bb.getIntSafe
            typMod <- bb.getIntSafe
          } yield Column(flags, name, dtId, typMod)
        }.toRight(DecodeError.WalBufferUnderflow)

        extension (c: Column) {
          def isKey: Boolean = (c.flags & 1) != 0
        }
      }

      def parse[A](bb: ByteBuffer): Either[DecodeError, LogicalReplication[A]] = {
        for {
          id <- bb.getIntSafe
          ns <- bb.getString
          name <- bb.getString
          replicaIdentity <- bb.getByteSafe
          numColumns <- bb.getShortSafe
        } yield (id, ns, name, replicaIdentity, numColumns)
      }.toRight(DecodeError.WalBufferUnderflow).flatMap { case (id, ns, name, replicaIdentity, numColumns) =>
        {
          (0 until numColumns).foldLeft[Either[DecodeError, List[Column]]](Right(Nil)) { (acc, _) =>
            acc.flatMap { acc =>
              Column.parse(bb).map(_ :: acc)
            }
          }
        }.map(xs => Relation(id, ns, name, replicaIdentity, xs.reverse))
      }
    }

    object Type {
      def parse[A](bb: ByteBuffer): Either[DecodeError, LogicalReplication[A]] = {
        for {
          id <- bb.getIntSafe
          ns <- bb.getString
          name <- bb.getString
        } yield Type(id, ns, name)
      }.toRight(DecodeError.WalBufferUnderflow)
    }

    object Insert {
      def parse[A: TupleDecoder](bb: ByteBuffer): Either[DecodeError, LogicalReplication[A]] = {
        (for {
          id <- bb.getIntSafe
          n <- bb.getByteSafe
        } yield id).toRight(DecodeError.WalBufferUnderflow).flatMap { id =>
          TupleData.parse(bb).map(Insert(id, _))
        }
      }
    }

    object Update {
      def parse[A: TupleDecoder](bb: ByteBuffer): Either[DecodeError, LogicalReplication[A]] = {
        (for {
          id <- bb.getIntSafe
          keyKind <- bb.getByteSafe
        } yield (id, keyKind)).toRight(DecodeError.WalBufferUnderflow).flatMap { case (id, keyKind) =>
          TupleData.parse(bb).flatMap { data =>
            if (keyKind == 'N') Right(Update(id, oldTuples = None, newTuples = data))
            else
              bb.getByteSafe.toRight(DecodeError.WalBufferUnderflow).flatMap { _ =>
                TupleData.parse(bb).flatMap { newData =>
                  keyKind match {
                    case 'K'   => Right(Update(id, oldTuples = Some(Left(data)), newTuples = newData))
                    case 'O'   => Right(Update(id, oldTuples = Some(Right(data)), newTuples = newData))
                    case other => Left(DecodeError.UnknownLogicalReplicationUpdateKind(other))
                  }
                }
              }
          }
        }
      }
    }

    object Delete {
      def parse[A: TupleDecoder](bb: ByteBuffer): Either[DecodeError, LogicalReplication[A]] = {
        (for {
          id <- bb.getIntSafe
          keyKind <- bb.getByteSafe
        } yield (id, keyKind)).toRight(DecodeError.WalBufferUnderflow).flatMap { case (id, keyKind) =>
          TupleData.parse(bb).flatMap { data =>
            keyKind match {
              case 'K'   => Right(Delete(id, Left(data)))
              case 'O'   => Right(Delete(id, Right(data)))
              case other => Left(DecodeError.UnknownLogicalReplicationUpdateKind(other))
            }
          }
        }
      }
    }

    object Truncate {
      def parse[A](bb: ByteBuffer): Either[DecodeError, LogicalReplication[A]] = {
        (for {
          numRels <- bb.getIntSafe
          option <- bb.getByteSafe
        } yield (numRels, option)).toRight(DecodeError.WalBufferUnderflow).flatMap { case (numRels, option) =>
          {
            (0 until numRels).foldLeft[Option[List[Int]]](Some(Nil)) { (acc, _) =>
              acc.flatMap { acc =>
                bb.getIntSafe.map(_ :: acc)
              }
            }
          }.toRight(DecodeError.WalBufferUnderflow)
            .map(xs => Truncate(option, xs.reverse))
        }
      }
    }

    object StreamStart {
      def parse[A](bb: ByteBuffer): Either[DecodeError, LogicalReplication[A]] = {
        for {
          xid <- bb.getIntSafe
          firstSegment <- bb.getByteSafe
        } yield StreamStart(xid, firstSegment == 1)
      }.toRight(DecodeError.WalBufferUnderflow)
    }

    object StreamCommit {
      def parse[A](bb: ByteBuffer): Either[DecodeError, LogicalReplication[A]] = {
        for {
          xid <- bb.getIntSafe
          flags <- bb.getByteSafe
          lsn <- bb.getLongSafe
          endLsn <- bb.getLongSafe
          ts <- bb.getLongSafe
        } yield StreamCommit(xid, flags, lsn, endLsn, ts)
      }.toRight(DecodeError.WalBufferUnderflow)
    }

    object StreamAbort {
      def parse[A](bb: ByteBuffer): Either[DecodeError, LogicalReplication[A]] = {
        for {
          xid <- bb.getIntSafe
          subXid <- bb.getIntSafe
        } yield StreamAbort(xid, subXid)
      }.toRight(DecodeError.WalBufferUnderflow)
    }
  }

}
