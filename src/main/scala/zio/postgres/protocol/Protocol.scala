package zio.postgres
package protocol

import zio.*
import zio.stream.*

import connection.*

import java.nio.ByteBuffer

trait Protocol {
  // TODO: the result should be something more sophisticated than raw packet
  def simpleQuery(query: String): ZStream[Any, Nothing, Packet.DataRow]
}

object Protocol {

  enum CmdResp {
    case Resp(packet: Packet)
    case Cmd(kind: CmdResp.Cmd.Kind)
  }

  object CmdResp {
    object Cmd {
      enum Kind {
        case SimpleQuery(query: String, q: Queue[Packet.DataRow])
      }

      extension (k: Kind) {
        def respQueue: Option[Queue[Packet.DataRow]] = k match {
          case Kind.SimpleQuery(_, q) => Some(q)
        }
      }
    }
  }

  enum State {
    case Ready
    case QueryRespond(respond: Option[Queue[Packet.DataRow]], next: List[CmdResp.Cmd])
  }

  case class Live(q: Queue[CmdResp.Cmd]) extends Protocol {
    import CmdResp._
    import CmdResp.Cmd._

    override def simpleQuery(query: String): ZStream[Any, Nothing, Packet.DataRow] =
      for {
        respQ <- ZStream.fromZIO(Queue.bounded[Packet.DataRow](10))
        _ <- ZStream.fromZIO(q.offer(Cmd(Kind.SimpleQuery(query, respQ))))
        res <- ZStream.fromQueue(respQ)
      } yield res
  }

  def sendCommand(outQ: Queue[ByteBuffer], kind: CmdResp.Cmd.Kind): UIO[Unit] = kind match {
    case CmdResp.Cmd.Kind.SimpleQuery(query, _) => outQ.offer(Packet.simpleQueryMessage(query)).unit
  }

  def handleProto(outQ: Queue[ByteBuffer]): (State, CmdResp) => UIO[State] = {
    import CmdResp._
    import CmdResp.Cmd._

    {
      // we are idle and got a new query to run
      case (State.Ready, cmd @ Cmd(kind)) => sendCommand(outQ, kind).as(State.QueryRespond(kind.respQueue, Nil))
      // we are reading the results for the previous query, but got a new to run - store it in the state
      case (State.QueryRespond(rq, next), cmd @ Cmd(_)) => ZIO.succeed(State.QueryRespond(rq, next ::: (cmd :: Nil)))

      // send current query result, the state is kept the same
      case (st @ State.QueryRespond(respQueue, _), Resp(dr @ Packet.DataRow(_))) =>
        respQueue.fold(ZIO.succeed(st))(_.offer(dr).as(st))

      // query results are over - shutdown the results queue, but keep the state, dropping the queue
      case (st @ State.QueryRespond(respQueue, next), Resp(Packet.Close(_, _))) =>
        respQueue.fold(ZIO.succeed(st))(_.shutdown.as(State.QueryRespond(None, next)))

      // previous query is complete, we have nothing to run next
      case (State.QueryRespond(_, Nil), Resp(Packet.ReadyForQuery(_))) => ZIO.succeed(State.Ready)
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
