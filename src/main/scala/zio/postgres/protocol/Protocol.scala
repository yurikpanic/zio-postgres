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
    case Ready(next: List[CmdResp.Cmd])
    // TODO: should we store a queue to send responses as a separate field in the state?
    case QueryRespond(next: List[CmdResp.Cmd])
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
      case (State.Ready(Nil), cmd @ Cmd(kind))      => sendCommand(outQ, kind).as(State.QueryRespond(cmd :: Nil))
      case (State.QueryRespond(next), cmd @ Cmd(_)) => ZIO.succeed(State.QueryRespond(next ::: (cmd :: Nil)))

      case (st @ State.QueryRespond(cur :: tl), Resp(dr @ Packet.DataRow(_))) =>
        cur.kind.respQueue.fold(ZIO.succeed(st))(_.offer(dr).as(st))

      case (st @ State.QueryRespond(cur :: tl), Resp(Packet.Close(_, _))) =>
        cur.kind.respQueue.fold(ZIO.succeed(st))(_.shutdown.as(st))

      case (State.QueryRespond(Nil), Resp(Packet.ReadyForQuery(_))) => ZIO.succeed(State.Ready(Nil))
      case (State.QueryRespond(h :: tl), Resp(Packet.ReadyForQuery(_))) =>
        sendCommand(outQ, h.kind).as(State.QueryRespond(h :: tl)) // TODO: what if we are not waiting for responses?

      case xx @ (s, _) => ZIO.succeedBlocking(println(xx)).as(s) // TODO
    }
  }

  def live(outQ: Queue[ByteBuffer], in: ZStream[Any, Connection.Error, Packet]): URIO[Scope, Protocol] =
    for {
      q <- Queue.unbounded[CmdResp.Cmd]
      _ <- ZStream
        .fromQueue(q)
        .merge(in.map(CmdResp.Resp(_)))
        .runFoldZIO(State.Ready(Nil))(handleProto(outQ))
        .fork
    } yield Live(q)

}
