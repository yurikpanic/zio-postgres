package zio.postgres
package protocol

import java.nio.ByteBuffer
import java.time.Instant
import java.time.temporal.ChronoUnit

import zio.*
import zio.stream.ZStream.HaltStrategy
import zio.stream.*

import connection.*
import decode.Decoder

trait Protocol {
  def simpleQuery[S, A](query: String)(using Decoder[S, Packet, A]): ZStream[Any, Protocol.Error, A]
  def standbyStatusUpdate(
      walWritten: Long,
      walFlushed: Long,
      walApplied: Long,
      clock: Instant,
      replyNow: Boolean = false
  ): UIO[Unit]
}

object Protocol {

  enum Error extends Throwable {
    case Backend(fields: Map[Error.Backend.Type, String])
    case Decode(error: decode.DecodeError)
    case ConnectionClosed
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

      final case class Reply[S, A](q: Queue[Either[Protocol.Error, Option[A]]], decoder: Decoder[S, Packet, A])

      enum Kind {
        case SimpleQuery[S, A](query: String, reply: Reply[S, A])
        case CopyData(data: Array[Byte])
      }

      extension (k: Kind) {
        def reply[S, A]: Option[Reply[S, A]] = k match {
          case Kind.SimpleQuery(_, reply: Reply[S, A]) => Some(reply)
          case _                                       => None
        }
      }
    }
  }

  enum State {
    case Ready
    case QueryRespond[S, A](respond: Option[CmdResp.Cmd.Reply[S, A]], state: Option[S], next: List[CmdResp.Cmd])
  }

  val pgEpoch = Instant.parse("2000-01-01T00:00:00Z")

  case class Live(q: Queue[CmdResp.Cmd]) extends Protocol {
    import CmdResp._
    import CmdResp.Cmd._

    override def simpleQuery[S, A](query: String)(using Decoder[S, Packet, A]): ZStream[Any, Protocol.Error, A] =
      for {
        respQ <- ZStream.fromZIO(Queue.bounded[Either[Protocol.Error, Option[A]]](10))
        _ <- ZStream.fromZIO(q.offer(Cmd(Kind.SimpleQuery(query, Reply(respQ, summon[Decoder[S, Packet, A]])))))
        res <- ZStream
          .fromQueueWithShutdown(respQ)
          .flatMap {
            case Left(err)      => ZStream.fail(err)
            case Right(Some(x)) => ZStream.succeed(x)
            case Right(None)    => ZStream.fromZIO(respQ.shutdown) *> ZStream.empty // this completes the stream
          }
      } yield res

    override def standbyStatusUpdate(
        walWritten: Long,
        walFlushed: Long,
        walApplied: Long,
        clock: Instant,
        replyNow: Boolean = false
    ): UIO[Unit] = q
      .offer(
        Cmd(
          Kind.CopyData(
            Packet.standbyStatusUpdate(
              walWritten,
              walFlushed,
              walApplied,
              ChronoUnit.MICROS.between(pgEpoch, clock),
              replyNow
            )
          )
        )
      )
      .unit
  }

  def sendCommand(outQ: Queue[ByteBuffer], kind: CmdResp.Cmd.Kind): UIO[Unit] = kind match {
    case CmdResp.Cmd.Kind.SimpleQuery(query, _) => outQ.offer(Packet.simpleQueryMessage(query)).unit
    case CmdResp.Cmd.Kind.CopyData(data)        => outQ.offer(Packet.copyDataMessage(data)).unit
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
      // TODO: perhaps we need to limit the state where we can send copy data.
      case (state, Cmd(kind @ Kind.CopyData(_))) => sendCommand(outQ, kind).as(state)

      // we are idle and got a new query to run
      case (State.Ready, cmd @ Cmd(kind)) => sendCommand(outQ, kind).as(State.QueryRespond(kind.reply, None, Nil))
      // we are reading the results for the previous query, but got a new to run - store it in the state
      case (State.QueryRespond(rq, s, next), cmd @ Cmd(_)) =>
        ZIO.succeed(State.QueryRespond(rq, s, next ::: (cmd :: Nil)))

      // send current query result, the state is kept the same
      case (st @ State.QueryRespond(Some(reply), s, next), Resp(packet)) if !reply.decoder.isDone(packet) =>
        reply.decoder.decode(s, packet).mapError(Error.Decode(_)).either.flatMap {
          case Left(err)             => reply.q.offer(Left(err)).as(st)
          case Right(s, a @ Some(_)) => reply.q.offer(Right(a)).as(st.copy(state = s))
          case Right(s, None)        => ZIO.succeed(st.copy(state = s))
        }

      // query results are over - complete the results queue, but keep the state, dropping the queue
      case (st @ State.QueryRespond(Some(reply), _, next), Resp(packet)) if reply.decoder.isDone(packet) =>
        reply.q.offer(Right(None)).as(State.QueryRespond(None, None, next))

      // error happend executing previous query, we have no new query to run
      case (State.QueryRespond(reply, _, Nil), Resp(Packet.Error(fields))) =>
        backendError(reply.map(_.q), fields).as(State.Ready)
      // error happend executing previous query, we already have a query to run next
      case (State.QueryRespond(reply, _, h :: tl), Resp(Packet.Error(fields))) =>
        sendCommand(outQ, h.kind) *> backendError(reply.map(_.q), fields).as(State.QueryRespond(h.kind.reply, None, tl))

      // previous query is complete, we have nothing to run next
      case (State.QueryRespond(_, _, Nil), Resp(Packet.ReadyForQuery(_))) =>
        ZIO.succeed(State.Ready)
      // previous query is complete, we already have a query to run next
      case (State.QueryRespond(_, s, h :: tl), Resp(Packet.ReadyForQuery(_))) =>
        sendCommand(outQ, h.kind).as(State.QueryRespond(h.kind.reply, s, tl))

      // ignore any other packets
      case (s, _) => ZIO.succeed(s)
    }
  }

  def live(outQ: Queue[ByteBuffer], in: ZStream[Any, Connection.Error, Packet]): URIO[Scope, Protocol] =
    for {
      q <- Queue.unbounded[CmdResp.Cmd]
      _ <- ZStream
        .fromQueue(q)
        .merge(in.map(CmdResp.Resp(_)), HaltStrategy.Either)
        .runFoldZIO(State.Ready)(handleProto(outQ))
        .tap {
          case State.QueryRespond(Some(reply), _, _) => reply.q.offer(Left(Error.ConnectionClosed))
          case _                                     => ZIO.unit
        }
        .tap(_ => q.shutdown)
        .forkScoped
    } yield Live(q)

}
