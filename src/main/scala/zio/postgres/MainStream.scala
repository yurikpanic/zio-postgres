package zio.postgres

import zio.*

import java.time.Instant

import connection.*
import protocol.*

object MainStream extends ZIOAppDefault {
  import Decoder.*

  override def run = (for {
    conn <- ZIO.service[Connection]

    proto <- conn.init(Some(Packet.ReplicationMode.Logical))
    res <- proto
      .simpleQuery[Wal.Message](
        """START_REPLICATION SLOT "testsub" LOGICAL 0/0 (proto_version '2', publication_names '"testpub"')"""
      )
      .tap { x =>
        Console.printLine(s"WAL data: $x")
      }
      .tap {
        case Wal.Message.PrimaryKeepAlive(walEnd, _, _) =>
          proto.standbyStatusUpdate(walEnd, walEnd, walEnd, Instant.now())
        case _ => ZIO.unit
      }
      .runCollect
    _ <- Console.printLine(s"Result: $res")
  } yield ()).provideSome[Scope](
    Connection.live,
    Parser.live,
    Auth.live,
    Socket.tcp,
    ZLayer.succeed(
      Config(host = "localhost", port = 5432, database = "test", user = "test", password = "test")
    )
  )
}
