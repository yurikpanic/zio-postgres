package zio.postgres
package protocol

import zio.*

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.immutable.ArraySeq
import scala.util.Try
import scala.util.chaining.*
import scala.util.control.NonFatal

import util.*

enum Packet {
  case Generic(`type`: Byte, payload: Chunk[Byte])
  case AuthRequest(kind: Packet.AuthRequest.Kind)
  case BackendKey(pid: Int, secret: Int)
  case ParameterStatus(name: String, value: String)
  case ReadyForQuery(status: Packet.ReadyForQuery.TransactionStatus)
  case RowDescription(fields: List[Packet.RowDescription.Field])
  case DataRow(fields: List[Option[Array[Byte]]])
  case CommandComplete(tag: String)
  case Error(fields: Map[Byte, String])
  case CopyBoth(overallFormat: Packet.FieldFormat, columnForamt: List[Packet.FieldFormat])
  case CopyData(data: Array[Byte])
}

object Packet {

  enum ParseError {
    case BufferUnderflow
    case UnknownAuthRequestKind(code: Int)
    case UnknownTransactionStatus(code: Byte)
    case UnknownCloseKind(code: Byte)
    case UnknownFormat(code: Short)
  }

  object AuthRequest {
    enum Kind {
      case Ok
      case KerberosV5
      case CleartextPassword
      case MD5Password(salt: Array[Byte])
      case SCMCredential
      case GSS
      case GSSContinue(data: Array[Byte])
      case SSPI
      case SASL(mechanisms: List[String])
      case SASLContinue(data: Array[Byte])
      case SASLFinal(data: Array[Byte])
    }

    object Kind {

      def parse(payload: Chunk[Byte]): Either[ParseError, Kind] = {
        val bb = ByteBuffer.wrap(payload.toArray).order(ByteOrder.BIG_ENDIAN)

        def restDataRaw: Either[ParseError, Array[Byte]] = {
          val data = new Array[Byte](bb.limit() - bb.position())
          Try(bb.get(data)).toEither.left.map(_ => ParseError.BufferUnderflow).map(_ => data)
        }

        Try(bb.getInt).toEither.left
          .map(_ => ParseError.BufferUnderflow)
          .flatMap {
            case 0 => Right(Ok)
            case 2 => Right(KerberosV5)
            case 3 => Right(CleartextPassword)
            case 5 =>
              restDataRaw.flatMap { salt =>
                if (salt.length != 4) Left(ParseError.BufferUnderflow)
                else Right(MD5Password(salt))
              }
            case 6  => Right(SCMCredential)
            case 7  => Right(GSS)
            case 8  => restDataRaw.map(GSSContinue(_))
            case 9  => Right(SSPI)
            case 10 => bb.getStrings.toRight(ParseError.BufferUnderflow).map(SASL(_))
            case 11 => restDataRaw.map(SASLContinue(_))
            case 12 => restDataRaw.map(SASLFinal(_))

            case other => Left(ParseError.UnknownAuthRequestKind(other))
          }
      }
    }
  }

  object ReadyForQuery {
    enum TransactionStatus {
      case Idle
      case InTransaction
      case FailedTransaction
    }

    def parse(payload: Chunk[Byte]): Either[ParseError, ReadyForQuery] = payload(0) match {
      case 'I'   => Right(ReadyForQuery(TransactionStatus.Idle))
      case 'T'   => Right(ReadyForQuery(TransactionStatus.InTransaction))
      case 'E'   => Right(ReadyForQuery(TransactionStatus.FailedTransaction))
      case other => Left(ParseError.UnknownTransactionStatus(other))
    }
  }

  object BackendKey {
    def parse(payload: Chunk[Byte]): Either[ParseError, Packet] = {
      val bb = ByteBuffer.wrap(payload.toArray).order(ByteOrder.BIG_ENDIAN)

      {
        for {
          pid <- Try(bb.getInt)
          secret <- Try(bb.getInt)
        } yield BackendKey(pid, secret)
      }.toEither.left.map(_ => ParseError.BufferUnderflow)
    }
  }

  object ParameterStatus {
    def parse(payload: Chunk[Byte]): Either[ParseError, Packet] = {
      val bb = ByteBuffer.wrap(payload.toArray).order(ByteOrder.BIG_ENDIAN)

      {
        for {
          name <- bb.getString
          value <- bb.getString
        } yield ParameterStatus(name, value)
      }.toRight(ParseError.BufferUnderflow)
    }
  }

  object RowDescription {

    final case class Field(
        name: String,
        colOid: Int,
        colNum: Short,
        typeOid: Int,
        typeSize: Short,
        typeModifier: Int,
        format: FieldFormat
    )

    object Field {
      def parse(bb: ByteBuffer): Either[ParseError, Field] = {
        for {
          name <- bb.getString
          colOid <- bb.getIntSafe
          colNum <- bb.getShortSafe
          typeOid <- bb.getIntSafe
          typeSize <- bb.getShortSafe
          typeModifier <- bb.getIntSafe
          format <- bb.getShortSafe
        } yield (
          name,
          colOid,
          colNum,
          typeOid,
          typeSize,
          typeModifier,
          format
        )
      }
        .toRight(ParseError.BufferUnderflow)
        .flatMap { case (name, colOid, colNum, typeOid, typeSize, typeModifier, format) =>
          FieldFormat.parse(format).map(Field(name, colOid, colNum, typeOid, typeSize, typeModifier, _))
        }

    }

    def parse(payload: Chunk[Byte]): Either[ParseError, Packet] = {
      val bb = ByteBuffer.wrap(payload.toArray).order(ByteOrder.BIG_ENDIAN)

      bb.getShortSafe
        .toRight(ParseError.BufferUnderflow)
        .flatMap { cnt =>
          (0 until cnt)
            .foldLeft[Either[ParseError, List[Field]]](Right(Nil)) { (acc, _) =>
              acc.flatMap(acc => Field.parse(bb).map(_ :: acc))
            }
            .map { xs =>
              RowDescription(xs.reverse)
            }
        }
    }
  }

  object DataRow {

    def parse(payload: Chunk[Byte]): Either[ParseError, Packet] = {
      val bb = ByteBuffer.wrap(payload.toArray).order(ByteOrder.BIG_ENDIAN)

      bb.getShortSafe
        .toRight(ParseError.BufferUnderflow)
        .flatMap { cnt =>
          (0 until cnt)
            .foldLeft[Either[ParseError, List[Option[Array[Byte]]]]](Right(Nil)) { (acc, _) =>
              acc.flatMap { acc =>
                (try {
                  val len = bb.getInt
                  if (len != -1) {
                    val arr = new Array[Byte](len)
                    bb.get(arr)
                    Right(Some(arr))
                  } else Right(None)
                } catch {
                  case NonFatal(_) => Left(ParseError.BufferUnderflow)
                }).map(_ :: acc)
              }
            }
            .map { xs =>
              DataRow(xs.reverse)
            }
        }
    }

  }

  object CommandComplete {

    def parse(payload: Chunk[Byte]): Either[ParseError, Packet] = {
      val bb = ByteBuffer.wrap(payload.toArray).order(ByteOrder.BIG_ENDIAN)

      bb.getString.map(Packet.CommandComplete(_)).toRight(ParseError.BufferUnderflow)
    }
  }

  object Error {
    def parse(payload: Chunk[Byte]): Either[ParseError, Packet] = {
      val bb = ByteBuffer.wrap(payload.toArray).order(ByteOrder.BIG_ENDIAN)

      def loop(acc: Map[Byte, String]): Either[ParseError, Map[Byte, String]] = {
        Try(bb.get()).toEither.left.map(_ => ParseError.BufferUnderflow).flatMap { t =>
          if (t == 0) Right(acc)
          else bb.getString.toRight(ParseError.BufferUnderflow).flatMap(value => loop(acc + (t -> value)))
        }
      }

      loop(Map.empty).map(Error(_))
    }
  }

  enum FieldFormat {
    case Textual
    case Binary
  }

  object FieldFormat {
    def parse(x: Short): Either[ParseError, FieldFormat] = x match {
      case 0 => Right(Textual)
      case 1 => Right(Binary)
      case x => Left(ParseError.UnknownFormat(x))
    }
    def parse(b: Byte): Either[ParseError, FieldFormat] = parse(b.toShort)
  }

  object CopyBoth {
    def parse(payload: Chunk[Byte]): Either[ParseError, Packet] = {
      val bb = ByteBuffer.wrap(payload.toArray).order(ByteOrder.BIG_ENDIAN)

      (for {
        overall <- bb.getByteSafe
        cnt <- bb.getShortSafe
        arr = new Array[Byte](cnt)
        _ <- Try(bb.get(arr)).toOption
      } yield (overall, arr.toList))
        .toRight(ParseError.BufferUnderflow)
        .flatMap { case (overall, cols) =>
          for {
            overall <- FieldFormat.parse(overall)
            cols <- cols.foldRight[Either[ParseError, List[FieldFormat]]](Right(Nil)) { (cur, acc) =>
              acc.flatMap { acc =>
                FieldFormat.parse(cur).map(_ :: acc)
              }
            }
          } yield CopyBoth(overall, cols)
        }
    }
  }

  object CopyData {

    def parse(payload: Chunk[Byte]): Either[ParseError, Packet] =
      Right(Packet.CopyData(payload.toArray))

  }

  def parse(tpe: Byte, payload: Chunk[Byte]): Either[ParseError, Packet] = {
    tpe match {
      case 'R' => AuthRequest.Kind.parse(payload).map(Packet.AuthRequest(_))
      case 'K' => BackendKey.parse(payload)
      case 'S' => ParameterStatus.parse(payload)
      case 'Z' => ReadyForQuery.parse(payload)
      case 'T' => RowDescription.parse(payload)
      case 'D' => DataRow.parse(payload)
      case 'C' => CommandComplete.parse(payload)
      case 'E' => Error.parse(payload)
      case 'W' => CopyBoth.parse(payload)
      case 'd' => CopyData.parse(payload)
      case _   => Right(Packet.Generic(tpe, payload))
    }
  }

  enum ReplicationMode {
    case Logical
    case Physical
  }

  def startupMessage(user: String, database: String, replication: Option[ReplicationMode] = None): ByteBuffer = {
    val replStr = replication.fold("false") {
      case ReplicationMode.Logical  => "database"
      case ReplicationMode.Physical => "true"
    }

    Gen.make(
      Field.Length,
      Field.Int32(196608),
      Field.String("user"),
      Field.String(user),
      Field.String("database"),
      Field.String(database),
      Field.String("application_name"),
      Field.String("zio-postgres"),
      Field.String("replication"),
      Field.String(replStr),
      Field.Byte(0) // terminator
    )
  }

  def passwordMessage(password: String): ByteBuffer =
    Gen.make(
      Field.Byte('p'),
      Field.Length,
      Field.String(password)
    )

  def saslInitialResponseMessage(mechanism: String, message: String): ByteBuffer = {
    val _message = message.getBytes(UTF_8)
    Gen.make(
      Field.Byte('p'),
      Field.Length,
      Field.String(mechanism),
      Field.Int32(_message.length),
      Field.Bytes(ArraySeq.unsafeWrapArray(_message))
    )
  }

  def saslResponseMessage(message: String): ByteBuffer = {
    val _message = message.getBytes(UTF_8)
    Gen.make(
      Field.Byte('p'),
      Field.Length,
      Field.Bytes(ArraySeq.unsafeWrapArray(_message))
    )
  }

  def simpleQueryMessage(query: String): ByteBuffer =
    Gen.make(
      Field.Byte('Q'),
      Field.Length,
      Field.String(query)
    )

  def copyDataMessage(data: Array[Byte]): ByteBuffer =
    Gen.make(
      Field.Byte('d'),
      Field.Length,
      Field.Bytes(ArraySeq.unsafeWrapArray(data))
    )

}
