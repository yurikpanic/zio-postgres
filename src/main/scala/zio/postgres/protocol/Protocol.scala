package zio.postgres
package protocol

import zio.*
import zio.stream.*

import connection.*

import java.nio.ByteBuffer

trait Protocol {
  def simpleQuery[A: Decoder](query: String): ZStream[Any, Protocol.Error, A]
}

object Protocol {

  enum Error {
    case Backend(fields: Map[Error.Backend.Type, String])
    case Decode(error: Decoder.Error)
  }

  object Error {
    object Backend {
      enum Type {
        case Severity
        case SqlState
        case Message
        case Detail
        case Hint
        case Position
        case InternalPosition
        case InternalQuery
        case Where
        case SchemaName
        case TableName
        case ColumnName
        case DataTypeName
        case ConstraintName
        case File
        case Line
        case Routine
        case Other(code: Byte)
      }

      object Type {
        def apply(code: Byte): Type = code match {
          case 'S'   => Severity
          case 'V'   => Severity
          case 'C'   => SqlState
          case 'M'   => Message
          case 'D'   => Detail
          case 'H'   => Hint
          case 'P'   => Position
          case 'q'   => InternalQuery
          case 'W'   => Where
          case 's'   => SchemaName
          case 't'   => TableName
          case 'c'   => ColumnName
          case 'd'   => DataTypeName
          case 'n'   => ConstraintName
          case 'F'   => File
          case 'L'   => Line
          case 'R'   => Routine
          case other => Other(other)
        }
      }
    }
  }

  enum CmdResp {
    case Resp(packet: Packet)
    case Cmd(kind: CmdResp.Cmd.Kind)
  }

  object CmdResp {

    object Cmd {

      final case class Reply[A](q: Queue[Either[Protocol.Error, Option[A]]], decoder: Decoder[A])

      enum Kind {
        case SimpleQuery[A](query: String, reply: Reply[A])
      }

      extension (k: Kind) {
        def reply[A]: Option[Reply[A]] = k match {
          case Kind.SimpleQuery(_, reply: Reply[A]) => Some(reply)
        }
      }
    }
  }

  enum State {
    case Ready
    case QueryRespond[A](respond: Option[CmdResp.Cmd.Reply[A]], next: List[CmdResp.Cmd])
  }

  case class Live(q: Queue[CmdResp.Cmd]) extends Protocol {
    import CmdResp._
    import CmdResp.Cmd._

    override def simpleQuery[A: Decoder](query: String): ZStream[Any, Protocol.Error, A] =
      for {
        respQ <- ZStream.fromZIO(Queue.bounded[Either[Protocol.Error, Option[A]]](10))
        _ <- ZStream.fromZIO(q.offer(Cmd(Kind.SimpleQuery(query, Reply(respQ, summon[Decoder[A]])))))
        res <- ZStream
          .fromQueueWithShutdown(respQ)
          .flatMap {
            case Left(err)      => ZStream.fail(err)
            case Right(Some(x)) => ZStream.succeed(x)
            case Right(None)    => ZStream.fromZIO(respQ.shutdown) *> ZStream.empty // this completes the stream
          }
        // res <- Decoder[A]
        // .decode(row)
        // .fold(err => ZStream.fail(Protocol.Error.Decode(err)), ZStream.succeed(_))
      } yield res
  }

  def sendCommand(outQ: Queue[ByteBuffer], kind: CmdResp.Cmd.Kind): UIO[Unit] = kind match {
    case CmdResp.Cmd.Kind.SimpleQuery(query, _) => outQ.offer(Packet.simpleQueryMessage(query)).unit
  }

  def handleProto(outQ: Queue[ByteBuffer]): (State, CmdResp) => UIO[State] = {
    import CmdResp._
    import CmdResp.Cmd._

    def backendError[A](
        respQueue: Option[Queue[Either[Protocol.Error, A]]],
        fields: Map[Byte, String]
    ) =
      respQueue.fold(ZIO.unit)(_.offer(Left(Error.Backend(fields.map { (k, v) =>
        Error.Backend.Type(k) -> v
      }))).unit)

    {
      // we are idle and got a new query to run
      case (State.Ready, cmd @ Cmd(kind)) => sendCommand(outQ, kind).as(State.QueryRespond(kind.reply, Nil))
      // we are reading the results for the previous query, but got a new to run - store it in the state
      case (State.QueryRespond(rq, next), cmd @ Cmd(_)) => ZIO.succeed(State.QueryRespond(rq, next ::: (cmd :: Nil)))

      // send current query result, the state is kept the same
      case (st @ State.QueryRespond(Some(reply), _), Resp(packet)) if reply.decoder.decode.isDefinedAt(packet) =>
        reply.q.offer(reply.decoder.decode(packet).left.map(Error.Decode(_)).map(Some(_))).as(st)

      // query results are over - complete the results queue, but keep the state, dropping the queue
      case (st @ State.QueryRespond(Some(reply), next), Resp(packet)) if reply.decoder.isDone(packet) =>
        reply.q.offer(Right(None)).as(State.QueryRespond(None, next))

      // error happend executing previous query, we have no new query to run
      case (State.QueryRespond(reply, Nil), Resp(Packet.Error(fields))) =>
        backendError(reply.map(_.q), fields).as(State.Ready)
      // error happend executing previous query, we already have a query to run next
      case (State.QueryRespond(reply, h :: tl), Resp(Packet.Error(fields))) =>
        sendCommand(outQ, h.kind) *> backendError(reply.map(_.q), fields).as(State.QueryRespond(h.kind.reply, tl))

      // previous query is complete, we have nothing to run next
      case (State.QueryRespond(_, Nil), Resp(Packet.ReadyForQuery(_))) =>
        ZIO.succeed(State.Ready)
      // previous query is complete, we already have a query to run next
      case (State.QueryRespond(_, h :: tl), Resp(Packet.ReadyForQuery(_))) =>
        sendCommand(outQ, h.kind).as(State.QueryRespond(h.kind.reply, tl))

      // ignore any other packets
      case (s, _) => ZIO.succeed(s)
    }
  }

  def live(outQ: Queue[ByteBuffer], in: ZStream[Any, Connection.Error, Packet]): URIO[Scope, Protocol] =
    for {
      q <- Queue.unbounded[CmdResp.Cmd]
      _ <- ZStream
        .fromQueue(q)
        .merge(in.map(CmdResp.Resp(_)))
        .runFoldZIO(State.Ready)(handleProto(outQ))
        .fork
    } yield Live(q)

}
