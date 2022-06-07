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
      enum Kind {
        case SimpleQuery(query: String, q: Queue[Either[Protocol.Error, Packet.DataRow]])
      }

      extension (k: Kind) {
        def respQueue: Option[Queue[Either[Protocol.Error, Packet.DataRow]]] = k match {
          case Kind.SimpleQuery(_, q) => Some(q)
        }
      }
    }
  }

  enum State {
    case Ready
    case QueryRespond(respond: Option[Queue[Either[Protocol.Error, Packet.DataRow]]], next: List[CmdResp.Cmd])
  }

  case class Live(q: Queue[CmdResp.Cmd]) extends Protocol {
    import CmdResp._
    import CmdResp.Cmd._

    override def simpleQuery[A: Decoder](query: String): ZStream[Any, Protocol.Error, A] =
      for {
        respQ <- ZStream.fromZIO(Queue.bounded[Either[Protocol.Error, Packet.DataRow]](10))
        _ <- ZStream.fromZIO(q.offer(Cmd(Kind.SimpleQuery(query, respQ))))
        row <- ZStream.fromQueue(respQ).flatMap {
          case Left(err) => ZStream.fail(err)
          case Right(x)  => ZStream.succeed(x)
        }
        res <- Decoder[A]
          .decode(row)
          .fold(err => ZStream.fail(Protocol.Error.Decode(err)), ZStream.succeed(_))
      } yield res
  }

  def sendCommand(outQ: Queue[ByteBuffer], kind: CmdResp.Cmd.Kind): UIO[Unit] = kind match {
    case CmdResp.Cmd.Kind.SimpleQuery(query, _) => outQ.offer(Packet.simpleQueryMessage(query)).unit
  }

  def handleProto(outQ: Queue[ByteBuffer]): (State, CmdResp) => UIO[State] = {
    import CmdResp._
    import CmdResp.Cmd._

    def backendError(respQueue: Option[Queue[Either[Protocol.Error, Packet.DataRow]]], fields: Map[Byte, String]) =
      respQueue.fold(ZIO.unit)(_.offer(Left(Error.Backend(fields.map { (k, v) =>
        Error.Backend.Type(k) -> v
      }))).unit)

    {
      // we are idle and got a new query to run
      case (State.Ready, cmd @ Cmd(kind)) => sendCommand(outQ, kind).as(State.QueryRespond(kind.respQueue, Nil))
      // we are reading the results for the previous query, but got a new to run - store it in the state
      case (State.QueryRespond(rq, next), cmd @ Cmd(_)) => ZIO.succeed(State.QueryRespond(rq, next ::: (cmd :: Nil)))

      // send current query result, the state is kept the same
      case (st @ State.QueryRespond(respQueue, _), Resp(dr @ Packet.DataRow(_))) =>
        respQueue.fold(ZIO.succeed(st))(_.offer(Right(dr)).as(st))

      // query results are over - shutdown the results queue, but keep the state, dropping the queue
      case (st @ State.QueryRespond(respQueue, next), Resp(Packet.Close(_, _))) =>
        respQueue.fold(ZIO.succeed(st))(_.shutdown.as(State.QueryRespond(None, next)))

      // error happend executing previous query, we have no new query to run
      case (State.QueryRespond(respQueue, Nil), Resp(Packet.Error(fields))) =>
        backendError(respQueue, fields).as(State.Ready)
      // error happend executing previous query, we already have a query to run next
      case (State.QueryRespond(respQueue, h :: tl), Resp(Packet.Error(fields))) =>
        sendCommand(outQ, h.kind) *> backendError(respQueue, fields).as(State.QueryRespond(h.kind.respQueue, tl))

      // previous query is complete, we have nothing to run next
      case (State.QueryRespond(_, Nil), Resp(Packet.ReadyForQuery(_))) =>
        ZIO.succeed(State.Ready)
      // previous query is complete, we already have a query to run next
      case (State.QueryRespond(_, h :: tl), Resp(Packet.ReadyForQuery(_))) =>
        sendCommand(outQ, h.kind).as(State.QueryRespond(h.kind.respQueue, tl))

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
