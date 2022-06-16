package zio.postgres
package example

import zio.*
import zio.postgres.protocol.Wal.LogicalReplication

import java.time.Instant

import connection.*
import protocol.*

object Main extends ZIOAppDefault {
  import Decoder.*

  val init = for {
    conn <- ZIO.service[Connection]
    proto <- conn.init(None)
    _ <- proto.simpleQuery("create table test(id integer primary key, value text, x integer)").runCollect
    _ <- proto.simpleQuery("create publication testpub for table test").runCollect
    slot <- proto
      .simpleQuery("select * from pg_create_logical_replication_slot('testsub', 'pgoutput')")(textValue ~ textValue)
      .runCollect
    _ <- Console.printLine(s"Replication slot created: $slot")
  } yield ()

  def stateEntry(id: String, value: LogicalReplication.Column, x: LogicalReplication.Column) = {
    val _id = id.toInt
    val _value = value match {
      case LogicalReplication.Column.Text(x) => Some(x)
      case _                                 => None
    }
    val _x = x match {
      case LogicalReplication.Column.Text(x) => Some(x.toInt)
      case _                                 => None
    }

    _id -> (_value -> _x)
  }

  val stream = for {
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
      .mapAccumZIO(Map.empty[Int, (Option[String], Option[Int])]) {
        case (
              acc,
              Wal.Message.XLogData(
                _,
                _,
                _,
                LogicalReplication.Insert(_, LogicalReplication.Column.Text(id) :: value :: x :: _)
              )
            ) =>
          val state = acc + stateEntry(id, value, x)
          Console.printLine(s"State: $state").as(state -> state)

        // This handles only updates that does not touch the key data
        case (
              acc,
              Wal.Message.XLogData(
                _,
                _,
                _,
                LogicalReplication.Update(_, None, LogicalReplication.Column.Text(id) :: value :: x :: _)
              )
            ) =>
          val state = acc + stateEntry(id, value, x)
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
    _ <- proto.simpleQuery("insert into test (id, value, x) values (1, 'aaa', 10)").runCollect
    _ <- proto.simpleQuery("insert into test (id, value) values (2, 'bbb')").runCollect
    _ <- proto.simpleQuery("insert into test (id, value, x) values (3, 'ccc', 30)").runCollect
    _ <- proto.simpleQuery("update test set x = 20 where id = 2").runCollect
    _ <- proto.simpleQuery("update test set value = null where id = 3").runCollect
  } yield ()

  override def run = {
    for {
      args <- getArgs
      _ <- {
        if (args.contains("--init")) init
        else stream zipPar queries
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
