package zio.postgres
package connection

import zio.*
import zio.stream.*

import java.io.IOException

import protocol._

trait Connection {
  def init: ZIO[Config & Scope, IOException, Protocol]
}

object Connection {
  def init: ZIO[Config & Scope & Connection, IOException, Protocol] =
    ZIO.serviceWithZIO[Connection](_.init)

  val live: URLayer[Socket, Connection] = ZLayer {
    ZIO.service[Socket].map { socket =>
      new Connection {
        override def init: ZIO[Config & Scope, IOException, Protocol] = {
          for {
            q <- Queue.unbounded[Byte]
            _ <- ZStream.fromQueue(q).run(socket.sink)
          } yield ()
          ???
        }
      }
    }
  }

}
