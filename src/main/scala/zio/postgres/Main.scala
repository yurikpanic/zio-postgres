package zio.postgres

import zio.*

import connection.*
import protocol.*

object Main extends ZIOAppDefault {
  import Decoder.*

  override def run = (for {
    conn <- ZIO.service[Connection]

    // proto <- conn.init(None)
    // res1 <- proto.simpleQuery("select * from test")(textValue ~ textValue.opt ~ textValue.opt).runCollect.either
    // _ <- Console.printLine(s"Result1: $res1")
    // res2 <- proto.simpleQuery[Packet.DataRow]("select * fro test").runCollect.either
    // _ <- Console.printLine(s"Result2: $res2")
    // res3 <- proto.simpleQuery[Packet.DataRow]("insert into test (value, x) values ('zzz', 42)").runCollect.either
    // _ <- Console.printLine(s"Result3: $res3")

    proto <- conn.init(Some(Packet.ReplicationMode.Logical))
    res <- proto
      .simpleQuery(
        """START_REPLICATION SLOT "testsub" LOGICAL 0/0 (proto_version '2', publication_names '"testpub"')"""
      )
      .runCollect
    _ <- Console.printLine(s"Result: $res")
    _ <- ZIO.sleep(10.seconds).forever
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
