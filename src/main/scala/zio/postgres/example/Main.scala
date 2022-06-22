package zio.postgres
package example

import java.time.Instant

import zio.*

import replication.Wal
import replication.Wal.LogicalReplication
import connection.*
import decode.Field
import protocol.Packet
import protocol.Parser

object Main extends ZIOAppDefault {
  import decode.Decoder.*

  val init = for {
    conn <- ZIO.service[Connection]
    proto <- conn.init(None)
    _ <- proto.simpleQuery("create table test(id integer primary key, value text, x integer)").runCollect
    _ <- proto.simpleQuery("create publication testpub for table test").runCollect
    slot <- proto
      .simpleQuery("select * from pg_create_logical_replication_slot('testsub', 'pgoutput')")(
        using Field.text ~ Field.text
      )
      .runCollect
    _ <- Console.printLine(s"Replication slot created: $slot")
  } yield ()

  val stream = for {
    conn <- ZIO.service[Connection]
    proto <- conn.init(Some(Packet.ReplicationMode.Logical))
    res <- proto
      .simpleQuery(
        """START_REPLICATION SLOT "testsub" LOGICAL 0/0 (proto_version '2', publication_names '"testpub"')"""
      )(using {
        import replication.Decoder.*
        replication.Decoder(Field.int ~ Field.text.opt ~ Field.int.opt)
      })
      .tap {
        case Wal.Message.PrimaryKeepAlive(walEnd, _, _) =>
          proto.standbyStatusUpdate(walEnd, walEnd, walEnd, Instant.now())
        case _ => ZIO.unit
      }
      .mapAccumZIO(Map.empty[Int, (Option[String], Option[Int])]) {
        case (
              acc,
              Wal.Message.XLogData(
                _,
                _,
                _,
                LogicalReplication.Insert(_, (id, value, x))
              )
            ) =>
          val state = acc + (id -> (value -> x))
          Console.printLine(s"State: $state").as(state -> state)

        // This handles only updates that does not touch the key data
        case (
              acc,
              Wal.Message.XLogData(
                _,
                _,
                _,
                LogicalReplication.Update(_, None, (id, value, x))
              )
            ) =>
          val state = acc + (id -> (value -> x))
          Console.printLine(s"State: $state").as(state -> state)

        case (acc, message) =>
          ZIO.succeed(acc -> acc)
      }
      .runCollect
    _ <- Console.printLine(s"Stream result: $res") // Not expected to reach here
  } yield ()

  val queries = for {
    conn <- ZIO.service[Connection]
    proto <- conn.init(None)
    id <- proto
      .simpleQuery("select coalesce(max(id), 0) + 1 from test")(using Field.int.single)
      .runLast
      .map(_.getOrElse(1))
    _ <- proto.simpleQuery(s"insert into test (id, value, x) values ($id, 'aaa', ${id * 10})").runCollect
    _ <- proto.simpleQuery(s"insert into test (id, value) values (${id + 1}, 'bbb')").runCollect
    _ <- proto.simpleQuery(s"insert into test (id, value, x) values (${id + 2}, 'ccc', ${(id + 2) * 10})").runCollect
    _ <- proto.simpleQuery(s"update test set x = ${(id + 1) * 10} where id = ${id + 1}").runCollect
    _ <- proto.simpleQuery(s"update test set value = null where id = ${id + 2}").runCollect
  } yield ()

  override def run = {
    for {
      args <- getArgs
      _ <- {
        if (args.contains("--init")) init
        else {
          if (args.contains("--just-stream")) stream
          else stream zipPar queries
        }
      }
    } yield ()
  }.provideSome[Scope & ZIOAppArgs](
    Connection.live,
    Parser.live,
    Auth.live,
    Socket.tcp,
    ZLayer.succeed(
      Config(host = "localhost", port = 5432, database = "test", user = "test", password = "test")
    )
  )

}
