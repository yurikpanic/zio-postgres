package zio.postgres
package example

import java.time.Instant

import scala.util.chaining.*

import zio.*

import connection.*
import ddl.Migration
import decode.Field
import protocol.Packet
import protocol.Parser
import replication.Wal
import replication.Wal.LogicalReplication

object Main extends ZIOAppDefault {
  import decode.Decoder.*

  val m = {
    import ddl.*

    migration(
      createTable(
        "test".public,
        column("id", Type.Int, primaryKey = true) ::
          column("value", Type.Text, nullable = false) ::
          Nil
      ),
      alterTable("test".public, addColumn("y", Type.Int)),
      alterTable("test".public, renameColumn("y", "x"))
    )
  }

  val init = for {
    conn <- ZIO.service[Connection]
    proto <- conn.init(None)
    // _ <- sch.migrate().provide(ZLayer.succeed(proto))
    // _ <- proto.simpleQuery("create table test(id integer primary key, value text not null, x integer)").runCollect
    // _ <- proto.simpleQuery("create publication testpub for table test").runCollect
    // slot <- proto
    //   .simpleQuery("select * from pg_create_logical_replication_slot('testsub', 'pgoutput')")(
    //     using Field.text ~ Field.text
    //   )
    //   .runCollect
    // _ <- Console.printLine(s"Replication slot created: $slot")
  } yield ()

  val stream = for {
    conn <- ZIO.service[Connection]
    proto <- conn.init(Some(Packet.ReplicationMode.Logical))
    _ <- proto
      .simpleQuery(
        """START_REPLICATION SLOT "testsub" LOGICAL 0/0 (proto_version '1', publication_names '"testpub"')"""
      )(using {
        import replication.Decoder.*
        message(proto)(Field.int ~ Field.text ~ Field.int.opt, Field.int.single)
      })
      .debug("Wal.LogicalReplication")
      .mapAccumZIO(Map.empty[Int, (String, Option[Int])]) {
        case (acc, LogicalReplication.Insert(_, (id, value, x))) =>
          val state = acc + (id -> (value -> x))
          Console.printLine(s"State [insert]: $state").as(state -> state)

        case (acc, LogicalReplication.Update(_, key, (id, value, x))) =>
          val state = key.fold(acc)(_.fold(identity, identity).pipe(acc - _)) +
            (id -> (value -> x))
          Console.printLine(s"State [update]: $state").as(state -> state)

        case (acc, LogicalReplication.Delete(_, key)) =>
          val state = acc - key.fold(identity, identity)
          Console.printLine(s"State [delete]: $state").as(state -> state)

        case (acc, message) =>
          ZIO.succeed(acc -> acc)
      }
      .runCollect
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
    _ <- proto.simpleQuery(s"update test set id = ${id + 3}, value = 'CCC' where id = ${id + 2}").runCollect
    _ <- proto.simpleQuery(s"delete from test where id = ${id + 1}").runCollect
    res <- proto
      .simpleQuery(s"select id, value, x from test where id >= ${id} and id <= ${id + 3}")(
        using Field.int ~ Field.text ~ Field.int.opt
      )
      .runCollect
    _ <- ZIO.foreach(res)(x => Console.printLine(s"DB state: $x"))
  } yield ()

  override def run = {
    ZIO.scoped {
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
    }
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
