package zio.postgres
package connection

import zio.*
import zio.stream.*

import java.io.IOException
import java.nio.ByteBuffer

import protocol._

trait Connection {
  def init: ZIO[Config, IOException, Protocol]
}

object Connection {
  def init: ZIO[Config & Connection, IOException, Protocol] =
    ZIO.serviceWithZIO[Connection](_.init)

  enum State {
    case Init
  }

  def handleConn(protoP: Promise[Nothing, Protocol]): (State, Parser.Packet) => UIO[State] = { (state, _) =>
    ZIO.succeed(state)
  }

  val live: URLayer[Socket & Parser, Connection] = ZLayer {
    for {
      socket <- ZIO.service[Socket]
      parser <- ZIO.service[Parser]
    } yield new Connection {
      override def init: ZIO[Config, IOException, Protocol] = {
        val prog = for {
          q <- Queue.unbounded[ByteBuffer]
          parser <- ZIO.service[Parser]
          protoP <- Promise.make[Nothing, Protocol]
          out = ZStream.fromQueue(q).run(socket.sink)
          in = socket.stream.via(parser.pipeline).run(ZSink.foldLeftZIO(State.Init)(handleConn(protoP)))
          res <- out
            .zipParRight(in)
            .zipParRight {
              for {
                cfg <- ZIO.service[Config]
                _ <- q.offer(Messages.startupMessage(user = cfg.user, database = cfg.database))
                res <- protoP.await
              } yield res
            }
        } yield res

        prog.provideSome[Config](ZLayer.succeed(parser))
      }
    }

  }

}
