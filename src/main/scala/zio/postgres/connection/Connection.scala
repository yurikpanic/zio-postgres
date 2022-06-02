package zio.postgres
package connection

import zio.*
import zio.stream.*

import java.io.IOException
import java.nio.ByteBuffer

import protocol._

trait Connection {
  def init: ZIO[Config, Connection.Error, Protocol]
}

object Connection {
  def init: ZIO[Config & Connection, Error, Protocol] =
    ZIO.serviceWithZIO[Connection](_.init)

  enum Error {
    case IO(cause: IOException)
    case Parse(error: Packet.ParseError)
  }

  enum State {
    case Init
  }

  case class Live(socket: Socket, parser: Parser) extends Connection {

    def handleConn(protoP: Promise[Nothing, Protocol]): (State, Packet) => UIO[State] = { (state, _) =>
      ZIO.succeed(state) // TODO
    }

    override def init: ZIO[Config, Error, Protocol] = {
      val prog = for {
        q <- Queue.unbounded[ByteBuffer]
        protoP <- Promise.make[Nothing, Protocol]
        out = ZStream.fromQueue(q).run(socket.sink).mapError(Error.IO(_))
        in = socket.stream
          .mapError(Error.IO(_))
          .via(new ZPipeline(Parser.pipeline.channel.mapError(Error.Parse(_))))
          .tap { p =>
            ZIO.succeedBlocking(println(s"=====packet===> $p"))
          }
          .run(ZSink.foldLeftZIO(State.Init)(handleConn(protoP)))
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

      prog.provideSome[Config](ZLayer.succeed(parser))
    }
  }

  val live: URLayer[Socket & Parser, Connection] = ZLayer {
    for {
      socket <- ZIO.service[Socket]
      parser <- ZIO.service[Parser]
    } yield Live(socket, parser)
  }

}
