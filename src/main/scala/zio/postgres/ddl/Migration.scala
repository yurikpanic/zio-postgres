package zio.postgres.ddl

import scala.compiletime.*
import scala.compiletime.ops.string.*
import scala.deriving.Mirror

import zio.*
import zio.postgres.protocol.Protocol
import zio.prelude.*

final case class Migration(queries: List[String])

object Migration {

  val empty = Migration(Nil)

  enum ApplyError extends Throwable {
    case RunQuery(query: String, err: Protocol.Error)
  }

  extension (m: Migration) {
    def toApply: ZIO[Protocol, ApplyError, Unit] =
      ZIO.foreach(m.queries)(q => Protocol.simpleQuery(q).runCollect.mapError(ApplyError.RunQuery(q, _))).unit
  }

  given Associative[Migration] = new {
    def combine(l: => Migration, r: => Migration): Migration = Migration(l.queries ::: r.queries)
  }

  sealed trait Column[Name <: String, Type]

  object Column {

    type FromNamesAndTypes[Names <: Tuple, Types <: Tuple] <: Tuple =
      (Names, Types) match {
        case (EmptyTuple, EmptyTuple) => EmptyTuple
        case (n *: nTail, t *: tTail) => Column[n, t] *: FromNamesAndTypes[nTail, tTail]
      }

    type TypeForName[Name <: String, Columns <: Tuple] =
      Columns match {
        case EmptyTuple        => Nothing
        case Column[Name, tpe] => tpe
        case _ *: tl           => TypeForName[Name, tl]
      }

    type Names[Columns <: Tuple] <: Tuple =
      Columns match {
        case EmptyTuple            => EmptyTuple
        case Column[name, _] *: tl => name *: Names[tl]
      }

    inline def pgTypes[Columns <: Tuple]: Map[String, PgType[?]] =
      inline erasedValue[Columns] match {
        case _: EmptyTuple =>
          Map.empty

        case _: (Column[name, tpe] *: tl) =>
          pgTypes[tl] + (constValue[name] -> summonInline[PgType[tpe]])
      }

    inline def ctors[Columns <: Tuple]: List[(String, PgType[?])] =
      inline erasedValue[Columns] match {
        case _: EmptyTuple => Nil

        case _: (Column[name, tpe] *: tl) =>
          (constValue[name] -> summonInline[PgType[tpe]]) :: ctors[tl]
      }
  }

  sealed trait NamedTable[Name <: String, T <: Product]

  object NamedTable {
    type FromNamesAndTypes[Names <: Tuple, Types <: Tuple] <: Tuple =
      (Names, Types) match {
        case (EmptyTuple, EmptyTuple)    => EmptyTuple
        case (n *: nTl, Table[t] *: tTl) => NamedTable[n, t] *: FromNamesAndTypes[nTl, tTl]
        case (_ *: nTl, _ *: tTl)        => FromNamesAndTypes[nTl, tTl]
      }

    type Names[NamedTables <: Tuple] <: Tuple =
      NamedTables match {
        case EmptyTuple                => EmptyTuple
        case NamedTable[name, _] *: tl => name *: Names[tl]
      }

    inline def ctors[NamedTables <: Tuple]: Map[String, TableCtor[?, ?]] =
      inline erasedValue[NamedTables] match {
        case _: EmptyTuple => Map.empty
        case _: (NamedTable[name, table] *: tl) =>
          ctors[tl] + (constValue[name] -> summonInline[TableCtor[name, table]])
      }

    inline def migratorTo[NameFrom <: String, From <: Product, ToNamesTables <: Tuple]
        : Map[(String, String), TableMigrator[?, ?, ?, ?]] =
      inline erasedValue[ToNamesTables] match {
        case _: EmptyTuple => Map.empty
        case _: (NamedTable[NameFrom, table] *: tl) =>
          Map(
            (constValue[NameFrom] -> constValue[NameFrom]) -> summonInline[
              TableMigrator[NameFrom, From, NameFrom, table]
            ]
          )
        case _: (NamedTable[_, _] *: tl) => migratorTo[NameFrom, From, tl]
      }

    inline def migrators[FromNamedTables <: Tuple, ToNamesTables <: Tuple]
        : Map[(String, String), TableMigrator[?, ?, ?, ?]] =
      inline erasedValue[FromNamedTables] match {
        case _: EmptyTuple => Map.empty
        case _: (NamedTable[name, table] *: tl) =>
          migrators[tl, ToNamesTables] ++ migratorTo[name, table, ToNamesTables]
      }
  }

  sealed trait RawCommand[Q <: String]

  object RawCommand {
    type FromTypes[Types <: Tuple] <: Tuple =
      Types match {
        case EmptyTuple   => EmptyTuple
        case Raw[q] *: tl => RawCommand[q] *: FromTypes[tl]
        case _ *: tl      => FromTypes[tl]
      }

    inline def queries[RawCommands <: Tuple]: List[String] =
      inline erasedValue[RawCommands] match {
        case _: EmptyTuple                => Nil
        case _: (RawCommand[value] *: tl) => constValue[value] :: queries[tl]
      }
  }

  sealed trait Extension[E <: String]

  object Extension {
    type FromTuple[Exts <: Tuple] <: Tuple =
      Exts match {
        case EmptyTuple => EmptyTuple
        case s *: tl    => Extension[s] *: FromTuple[tl]
      }

    type FromTypes[Types <: Tuple] <: Tuple =
      Types match {
        case EmptyTuple           => EmptyTuple
        case Extensions[es] *: tl => Tuple.Concat[FromTuple[es], FromTypes[tl]]
        case _ *: tl              => FromTypes[tl]
      }

    inline def extensions[ETs <: Tuple]: List[String] =
      inline erasedValue[ETs] match {
        case _: EmptyTuple            => Nil
        case _: (Extension[es] *: tl) => constValue[es] :: extensions[tl]
      }
  }

  sealed trait NamedPublication[Name <: String, Tables <: Tuple]

  object NamedPublication {

    type ExtractTables[T] <: Tuple = T match {
      case EmptyTuple => EmptyTuple
      case t *: tl    => t *: tl
      case s & String => s *: EmptyTuple
    }

    type FromNamesTypesAndTables[Names <: Tuple, Types <: Tuple] <: Tuple =
      (Names, Types) match {
        case (EmptyTuple, EmptyTuple) => EmptyTuple

        case (n *: nTl, Publication[ts] *: tTl) =>
          NamedPublication[n, ExtractTables[ts]] *: FromNamesTypesAndTables[nTl, tTl]

        case (_ *: nTl, _ *: tTl) => FromNamesTypesAndTables[nTl, tTl]
      }

    inline def checkTable[Table, AvailTables <: Tuple]: Unit =
      inline erasedValue[AvailTables] match {
        case _: EmptyTuple =>
          error("Table " + constValue[Table] + " is not present in the schema - can not use it for publication")
        case _: (Table *: tl) => ()
        case _: (_ *: tl)     => checkTable[Table, tl]
      }

    inline def checkTablesPresent[PubTables <: Tuple, AvailTables <: Tuple]: Unit =
      inline erasedValue[PubTables] match {
        case _: EmptyTuple  => ()
        case _: (pub *: tl) => checkTable[pub, AvailTables]; checkTablesPresent[tl, AvailTables]

      }

    inline def publications[Ps <: Tuple, AllTableNames <: Tuple]: Map[String, List[String]] =
      inline erasedValue[Ps] match {
        case _: EmptyTuple => Map.empty

        case _: (NamedPublication[name, tables] *: tl) =>
          checkTablesPresent[tables, AllTableNames]

          publications[tl, AllTableNames] + (constValue[name] ->
            summonInline[Tuple.Union[tables] <:< String]
              .substituteCo(constValueTuple[tables].toList))
      }
  }

  inline def tMigration[From <: Product, To <: Product](using
      mFrom: Mirror.ProductOf[From],
      mTo: Mirror.ProductOf[To],
      evFromNamesStr: Tuple.Union[mFrom.MirroredElemLabels] <:< String,
      evToNamesStr: Tuple.Union[mTo.MirroredElemLabels] <:< String
  ): List[String] = {
    val fromNames: Set[String] = evFromNamesStr.substituteCo(constValueTuple[mFrom.MirroredElemLabels].toList).toSet
    val toNames: Set[String] = evToNamesStr.substituteCo(constValueTuple[mTo.MirroredElemLabels].toList).toSet

    type ToColumns = Column.FromNamesAndTypes[mTo.MirroredElemLabels, mTo.MirroredElemTypes]
    val toPgTypes = Column.pgTypes[ToColumns]

    toNames.diff(fromNames).map(s => s"ADD COLUMN $s ${toPgTypes(s).renderCtor}").toList :::
      fromNames.diff(toNames).map(s => s"DROP COLUMN $s").toList
  }

  trait TableMigrator[NameFrom <: String, From <: Product, NameTo <: String, To <: Product] {
    def render: List[String]
  }

  object TableMigrator {
    inline given [NameFrom <: String, From <: Product, NameTo <: String, To <: Product](using
        mFrom: Mirror.ProductOf[From],
        mTo: Mirror.ProductOf[To],
        evFromNamesStr: Tuple.Union[mFrom.MirroredElemLabels] <:< String,
        evToNamesStr: Tuple.Union[mTo.MirroredElemLabels] <:< String
    ): TableMigrator[NameFrom, From, NameTo, To] = new {

      override def render: List[String] = tMigration[From, To].map(s"ALTER TABLE ${constValue[NameFrom]} " + _)
    }
  }

  trait TableCtor[Name <: String, T <: Product] {
    def render: String
  }

  object TableCtor {
    inline given [Name <: String, T <: Product](using mm: Mirror.ProductOf[T]): TableCtor[Name, T] = new {
      type Columns = Column.FromNamesAndTypes[mm.MirroredElemLabels, mm.MirroredElemTypes]
      val columnCtors = Column.ctors[Columns]

      override def render: String = "CREATE TABLE " + constValue[Name] +
        columnCtors
          .map { (name, tpe) => s"$name ${tpe.renderCtor}" }
          .mkString("(", ",", ")")
    }
  }

  inline def migration[From <: Product, To <: Product](using
      mFrom: Mirror.ProductOf[From],
      mTo: Mirror.ProductOf[To],
      evTabFromS: Tuple.Union[
        NamedTable.Names[NamedTable.FromNamesAndTypes[mFrom.MirroredElemLabels, mFrom.MirroredElemTypes]]
      ] <:< String,
      evTabToS: Tuple.Union[
        NamedTable.Names[NamedTable.FromNamesAndTypes[mTo.MirroredElemLabels, mTo.MirroredElemTypes]]
      ] <:< String
  ): Migration = {
    val fromExtensions = Extension.extensions[Extension.FromTypes[mFrom.MirroredElemTypes]]
    val toExtensions = Extension.extensions[Extension.FromTypes[mTo.MirroredElemTypes]]

    val newExtensions = toExtensions.diff(fromExtensions)
    val droppedEtensions = fromExtensions.diff(toExtensions)

    val migrateExtensions = newExtensions.map(s => s"CREATE EXTENSION $s") :::
      droppedEtensions.map(s => s"DROP EXTENSION $s")

    val fromTableNames = evTabFromS
      .substituteCo(
        constValueTuple[
          NamedTable.Names[NamedTable.FromNamesAndTypes[mFrom.MirroredElemLabels, mFrom.MirroredElemTypes]]
        ].toList
      )

    val toTableNames = evTabToS
      .substituteCo(
        constValueTuple[
          NamedTable.Names[NamedTable.FromNamesAndTypes[mTo.MirroredElemLabels, mTo.MirroredElemTypes]]
        ].toList
      )

    val newTables = toTableNames.diff(fromTableNames)
    val retainedTables = toTableNames.intersect(fromTableNames)
    val droppedTables = fromTableNames.diff(toTableNames)

    val tableCtors = NamedTable.ctors[NamedTable.FromNamesAndTypes[mTo.MirroredElemLabels, mTo.MirroredElemTypes]]

    val tableMigrators = NamedTable.migrators[NamedTable.FromNamesAndTypes[
      mFrom.MirroredElemLabels,
      mFrom.MirroredElemTypes
    ], NamedTable.FromNamesAndTypes[mTo.MirroredElemLabels, mTo.MirroredElemTypes]]

    val migrateTables = newTables.toList.map(tableCtors(_).render) :::
      retainedTables.toList.flatMap(x => tableMigrators(x -> x).render) :::
      droppedTables.toList.map(s => s"DROP TABLE $s")

    val fromPubs = NamedPublication
      .publications[NamedPublication.FromNamesTypesAndTables[
        mFrom.MirroredElemLabels,
        mFrom.MirroredElemTypes,
      ], NamedTable.Names[NamedTable.FromNamesAndTypes[mFrom.MirroredElemLabels, mFrom.MirroredElemTypes]]]
    val toPubs = NamedPublication
      .publications[NamedPublication.FromNamesTypesAndTables[
        mTo.MirroredElemLabels,
        mTo.MirroredElemTypes,
      ], NamedTable.Names[NamedTable.FromNamesAndTypes[mTo.MirroredElemLabels, mTo.MirroredElemTypes]]]

    val newPubNames = toPubs.keySet.diff(fromPubs.keySet)
    val droppedPubNames = fromPubs.keySet.diff(toPubs.keySet)

    val migratePublicatins =
      newPubNames.toList.map(s => s"CREATE PUBLICATION $s FOR TABLE ${toPubs(s).mkString(", ")}") :::
        droppedPubNames.toList.map(s => s"DROP PUBLICATION $s")

    val rawCommadQueries = RawCommand.queries[RawCommand.FromTypes[mTo.MirroredElemTypes]]

    Migration(
      migrateExtensions :::
        migrateTables :::
        migratePublicatins :::
        rawCommadQueries
    )
  }

}
