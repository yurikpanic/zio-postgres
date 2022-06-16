package zio.postgres
package connection

import zio.*
import zio.stream.*

import java.io.IOException
import java.nio.ByteBuffer

import protocol._

trait Connection {
  def init(replication: Option[Packet.ReplicationMode]): ZIO[Config & Scope, Connection.Error, Protocol]
}

object Connection {
  def init(replication: Option[Packet.ReplicationMode]): ZIO[Config & Scope & Connection, Error, Protocol] =
    ZIO.serviceWithZIO[Connection](_.init(replication))

  enum Error {
    case IO(cause: IOException)
    case Parse(error: Packet.ParseError)
    case Auth(error: connection.Auth.Error)
    case Unexpected(message: String)
  }

  enum State {
    case Init
    case Run(protocol: Protocol)
  }

  case class Live(socket: Socket, parser: Parser, auth: Auth) extends Connection {

    def handleConn(
        outQ: Queue[ByteBuffer],
        packets: ZStream[Any, Error, Packet]
    ): (State, Packet) => URIO[Parser & Scope, State] = {
      case (State.Init, Packet.ReadyForQuery(Packet.ReadyForQuery.TransactionStatus.Idle)) =>
        Protocol.live(outQ, packets).map(State.Run(_))

      case (State.Init, _)       => ZIO.succeed(State.Init)
      case (r @ State.Run(_), _) => ZIO.succeed(r)
    }

    override def init(replication: Option[Packet.ReplicationMode]): ZIO[Config & Scope, Error, Protocol] = {
      val prog = for {
        q <- Queue.unbounded[ByteBuffer]
        cfg <- ZIO.service[Config]
        conn <- socket.connect(cfg.host, cfg.port).mapError(Error.IO(_))
        _ <- ZStream.fromQueue(q).run(conn._1).mapError(Error.IO(_)).forkScoped
        packets <- conn._2
          .mapError(Error.IO(_))
          .via(new ZPipeline(Parser.pipeline.channel.mapError(Error.Parse(_))))
          .debug("packet")
          .broadcast(2, 100)
        in = packets(0)
          .via(new ZPipeline(Auth.pipeline(q).channel.mapError(Error.Auth(_))))
          .runFoldWhileZIO(State.Init) {
            case State.Run(_) => false
            case _            => true
          }(handleConn(q, packets(1)))
          .flatMap {
            case State.Run(p) => ZIO.succeed(p)
            case state        => ZIO.fail(Error.Unexpected(s"Connection initialization is incomplete $state"))
          }
        res <- in
          .zipParLeft {
            for {
              cfg <- ZIO.service[Config]
              _ <- q.offer(Packet.startupMessage(user = cfg.user, database = cfg.database, replication = replication))
            } yield ()
          }
      } yield res

      prog.provideSome[Config & Scope](ZLayer.succeed(parser) ++ ZLayer.succeed(auth))
    }
  }

  val live: URLayer[Socket & Parser & Auth, Connection] = ZLayer {
    for {
      socket <- ZIO.service[Socket]
      parser <- ZIO.service[Parser]
      auth <- ZIO.service[Auth]
    } yield Live(socket, parser, auth)
  }

}
