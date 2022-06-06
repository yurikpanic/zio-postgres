package zio.postgres.protocol

import zio.*

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets.UTF_8
import scala.util.Try
import scala.util.chaining.*
import scala.util.control.NonFatal

enum Packet {
  case Generic(`type`: Byte, payload: Chunk[Byte])
  case AuthRequest(kind: Packet.AuthRequest.Kind)
  case BackendKey(pid: Int, secret: Int)
  case ParameterStatus(name: String, value: String)
  case ReadyForQuery(status: Packet.ReadyForQuery.TransactionStatus)
  case RowDescription(fields: List[Packet.RowDescription.Field])
  case DataRow(fields: List[Option[Array[Byte]]])
  case Close(kind: Packet.Close.Kind, name: String)
}

object Packet {

  enum ParseError {
    case BufferUnderflow
    case UnknownAuthRequestKind(code: Int)
    case UnknownTransactionStatus(code: Byte)
    case UnknownCloseKind(code: Byte)
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
        format: Short
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
        } yield Field(
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

  object Close {
    enum Kind {
      case PreparedStatemen
      case Portal
    }

    def parse(payload: Chunk[Byte]): Either[ParseError, Packet] = {
      val bb = ByteBuffer.wrap(payload.toArray).order(ByteOrder.BIG_ENDIAN)

      (bb.get() match {
        case 'S' => Right(Kind.PreparedStatemen)
        case 'P' => Right(Kind.Portal)
        case x   => Left(ParseError.UnknownCloseKind(x))
      }).flatMap { kind =>
        bb.getString.map(Packet.Close(kind, _)).toRight(ParseError.BufferUnderflow)
      }
    }
  }

  def parse(tpe: Byte, payload: Chunk[Byte]): Either[ParseError, Packet] = {
    tpe match {
      case 'R' => AuthRequest.Kind.parse(payload).map(Packet.AuthRequest(_))
      case 'K' => BackendKey.parse(payload)
      case 'S' => ParameterStatus.parse(payload)
      case 'Z' => ReadyForQuery.parse(payload)
      case 'T' => RowDescription.parse(payload)
      case 'D' => DataRow.parse(payload)
      case 'C' => Close.parse(payload)
      case _   => Right(Packet.Generic(tpe, payload))
    }
  }

  def startupMessage(user: String, database: String): ByteBuffer =
    Gen.make(
      Field.Length,
      Field.Int32(196608),
      Field.String("user"),
      Field.String(user),
      Field.String("database"),
      Field.String(database),
      Field.String("application_name"),
      Field.String("zio-postgres"),
      Field.Byte(0) // terminator
    )

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
      Field.Bytes(_message.toIndexedSeq)
    )
  }

  def saslResponseMessage(message: String): ByteBuffer = {
    val _message = message.getBytes(UTF_8)
    Gen.make(
      Field.Byte('p'),
      Field.Length,
      Field.Bytes(_message.toIndexedSeq)
    )
  }

  def simpleQueryMessage(query: String): ByteBuffer =
    Gen.make(
      Field.Byte('Q'),
      Field.Length,
      Field.String(query)
    )

  extension (bb: ByteBuffer) {
    def getString: Option[String] = {
      val pos = bb.position()
      def loop(acc: List[Byte]): Option[List[Byte]] = Try(bb.get).toOption.flatMap {
        case 0 => Some(acc)
        case x => loop(x :: acc)
      }

      loop(Nil)
        .map(_.reverse.toArray.pipe(new String(_, UTF_8)))
        .orElse {
          bb.position(pos) // reset position on parse failure
          None
        }
    }

    // zero terminated strings list
    def getStrings: Option[List[String]] = {
      val pos = bb.position()

      def loop(acc: List[String]): Option[List[String]] = {
        val pos = bb.position()
        Try(bb.get).toOption.flatMap {
          case 0 => Some(acc)
          case _ =>
            bb.position(pos)
            getString.map(_ :: acc).flatMap(loop(_))
        }
      }

      loop(Nil)
        .map(_.reverse)
        .orElse {
          bb.position(pos) // reset position on parse failure
          None
        }
    }

    def getShortSafe: Option[Short] = Try(bb.getShort).toOption
    def getIntSafe: Option[Int] = Try(bb.getInt).toOption
    def getLongSafe: Option[Long] = Try(bb.getLong).toOption

  }

}
