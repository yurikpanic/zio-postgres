package zio.postgres
package connection

import zio.*
import zio.stream.*

import java.io.IOException
import java.nio.ByteBuffer

import protocol._

trait Connection {
  def init: ZIO[Config & Scope, Connection.Error, Protocol]
}

object Connection {
  def init: ZIO[Config & Scope & Connection, Error, Protocol] =
    ZIO.serviceWithZIO[Connection](_.init)

  enum Error {
    case IO(cause: IOException)
    case Parse(error: Packet.ParseError)
    case Auth(error: connection.Auth.Error)
  }

  enum State {
    case Init
  }

  case class Live(socket: Socket, parser: Parser, auth: Auth) extends Connection {

    def handleConn(protoP: Promise[Nothing, Protocol]): (State, Packet) => UIO[State] = { (state, p) =>
      ZIO.succeed(state) // TODO
    }

    override def init: ZIO[Config & Scope, Error, Protocol] = {
      val prog = for {
        q <- Queue.unbounded[ByteBuffer]
        protoP <- Promise.make[Nothing, Protocol]
        out = ZStream.fromQueue(q).run(socket.sink).mapError(Error.IO(_)).map(x => println(s"out done"))
        in = socket.stream
          .mapError(Error.IO(_))
          .via(new ZPipeline(Parser.pipeline.channel.mapError(Error.Parse(_))))
          .tap { p =>
            ZIO.succeedBlocking(println(s"=====packet===> $p"))
          }
          .via(new ZPipeline(Auth.pipeline(q).channel.mapError(Error.Auth(_))))
          .run(ZSink.foldLeftZIO(State.Init)(handleConn(protoP)))
          .map(x => println(s"in done: $x"))
        res <- out
          .zipParRight(in)
          .zipParRight {
            for {
              cfg <- ZIO.service[Config]
              _ <- q.offer(Packet.startupMessage(user = cfg.user, database = cfg.database))
              res <- protoP.await
            } yield res
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
