package zio.postgres
package example

import java.time.Instant

import scala.util.chaining.*

import zio.*
import zio.prelude.*

import connection.*
import decode.Field
import protocol.Packet
import protocol.Parser
import replication.Wal
import replication.Wal.LogicalReplication
import zio.postgres.example.Main.Schema.TestV3
import zio.postgres.decode.Decoder

object Main extends ZIOAppDefault {
  import decode.Decoder.*

  object Schema {
    import ddl.*

    final case class SchemaV0()

    type PostgisExts = (
        "postgis",
        "postgis_raster",
        "postgis_topology",
        "postgis_sfcgal",
        "fuzzystrmatch",
        "postgis_tiger_geocoder"
    )

    type AddressStandardizerExts = (
        "address_standardizer",
        "address_standardizer_data_us"
    )

    final case class TestV1(id: PrimaryKey[Int], value: String)
    final case class SchemaV1(
        test: Table[TestV1],
        postgis: Extensions[PostgisExts],
        addrStd: Extensions[AddressStandardizerExts]
    )

    final case class TestV2(id: PrimaryKey[Int], value: String, y: Option[Int])
    final case class DummyV2(zzz: Int)
    final case class SchemaV2(
        test: Table[TestV2],
        dummy: Table[DummyV2],
        postgis: Extensions[PostgisExts],
        addrStd: Extensions[AddressStandardizerExts]
    )

    final case class TestV3(id: PrimaryKey[Int], value: String, x: Option[Int])
    final case class SchemaV3(
        test: Table[TestV3] Hawing ("x" RenamedFrom "y"),
        testpub: Publication["test"],
        testsub: ReplicationSlot["pgoutput"],
        postgis: Extensions[PostgisExts]
    )

    val m0 = Migration.migration[SchemaV0, SchemaV1]
    val m1 = Migration.migration[SchemaV1, SchemaV2]
    val m2 = Migration.migration[SchemaV2, SchemaV3]

    val migration = m0 combine m1 combine m2

  }

  val init = for {
    conn <- ZIO.service[Connection]
    proto <- conn.init(None)
    _ <- Console.printLine("Migration:")
    _ <- ZIO.foreach(Schema.migration.queries)(Console.printLine(_))
    _ <- Schema.migration.toApply.provide(ZLayer.succeed(proto))
  } yield ()

  val stream = for {
    conn <- ZIO.service[Connection]
    proto <- conn.init(Some(Packet.ReplicationMode.Logical))
    _ <- proto
      .simpleQuery(
        """START_REPLICATION SLOT "testsub" LOGICAL 0/0 (proto_version '1', publication_names '"testpub"')"""
      )(using {
        import replication.Decoder.*
        import replication.Decoder.TupleDecoder.given

        // message(proto)(Field.int ~ Field.text ~ Field.int.opt, Field.int.single)
        messageForTable[Schema.TestV3](proto)
      })
      .debug("Wal.LogicalReplication")
      .mapAccumZIO(Map.empty[Int, (String, Option[Int])]) {
        case (acc, LogicalReplication.Insert(_, test)) =>
          val state = acc + (test.id.value -> (test.value -> test.x))
          Console.printLine(s"State [insert]: $state").as(state -> state)

        case (acc, LogicalReplication.Update(_, key, test)) =>
          val state = key.fold(acc)(_.fold(identity, identity).pipe(acc - _._1)) +
            (test.id.value -> (test.value -> test.x))
          Console.printLine(s"State [update]: $state").as(state -> state)

        case (acc, LogicalReplication.Delete(_, key)) =>
          val state = acc - key.fold(_._1, _._1)
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
