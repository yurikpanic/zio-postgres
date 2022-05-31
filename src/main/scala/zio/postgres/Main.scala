package zio.postgres

import zio.*

import connection.*
import protocol.*

object Main extends ZIOAppDefault {
  override def run = (for {
    conn <- ZIO.service[Connection]
    _ <- conn.init
  } yield ()).provideSome[Scope](
    Connection.live,
    Parser.live,
    Socket.tcp,
    ZLayer.succeed(Config(host = "localhost", port = 5432, database = "test", user = "test", password = "test"))
  )
}
