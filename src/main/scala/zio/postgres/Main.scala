package zio.postgres

import zio.*

import connection.*
import protocol.*

object Main extends ZIOAppDefault {
  override def run = (for {
    conn <- ZIO.service[Connection]
    proto <- conn.init
    res1 <- proto.simpleQuery("select * from test").runCollect.either
    _ <- Console.printLine(s"Result1: $res1")
    res2 <- proto.simpleQuery("select * fro test").runCollect.either
    _ <- Console.printLine(s"Result2: $res2")
  } yield ()).provideSome[Scope](
    Connection.live,
    Parser.live,
    Auth.live,
    Socket.tcp,
    ZLayer.succeed(
      Config(host = "localhost", port = 5432, database = "test", user = "test_md5", password = "test_md5")
    )
    // ZLayer.succeed(
    //   Config(host = "localhost", port = 5432, database = "test", user = "test_plain", password = "test_plain")
    // )
    // ZLayer.succeed(
    // Config(host = "localhost", port = 5432, database = "test", user = "test", password = "test")
    // )
  )
}
