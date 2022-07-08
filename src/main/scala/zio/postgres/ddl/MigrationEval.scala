package zio.postgres
package ddl

import zio.*
import zio.prelude.*

import eval.*
import protocol.Protocol

object MigrationEval {
  import Migration.*

  extension (col: Schema.Table.Column) {
    def alter(op: Alter.Table.Alter.Operation): Either[ValidationError, Schema.Table.Column] = {
      import Alter.Table.Alter.Operation

      op match {
        case Operation.SetType(tpe)     => Right(col.copy(dataType = tpe)) // TODO: check if such change is possible
        case Operation.SetDefault(expr) => Left(ValidationError.Oops) // TODO: implement default values for tables
        case Operation.DropDefault      => Left(ValidationError.Oops)
        case Operation.SetNotNull       => Right(col.copy(nullable = false))
        case Operation.DropNotNull      => Right(col.copy(nullable = true))
      }
    }
  }

  extension (table: Schema.Table) {

    def alter(sch: Schema, op: Migration.Alter.Table.Operation): Either[ValidationError, Schema.Table] = {
      import Migration.Alter.Table.Operation.*
      import Migration.Alter.Table.Rename.{Target => RT}
      import Migration.Alter.Table.Add.{Target => AT}
      import Migration.Alter.Table.Drop.{Target => DT}
      import Migration.Alter.Table.Alter.{Target => AlterTarget}

      op match {
        case Add(AT.Column(name, dataType, nullable)) =>
          table.addedColumn(name, dataType, nullable).left.map(ValidationError.SchemaUpdate(_))

        case Rename(RT.Column(name, to))     => table.renamedColumn(name, to).left.map(ValidationError.SchemaUpdate(_))
        case Rename(RT.Constraint(name, to)) => Left(ValidationError.Oops)
        case Rename(RT.Table(to)) =>
          val newName = table.name.copy(name = to)
          sch.tables.get(newName) match {
            case None    => Right(table.copy(name = newName))
            case Some(_) => Left(ValidationError.SchemaUpdate(Schema.UpdateError.RealtionAlreadyExists(newName)))
          }

        // TODO: flags
        case Drop(DT.Column(name, _))     => table.droppedColumn(name).left.map(ValidationError.SchemaUpdate(_))
        case Drop(DT.Constraint(name, _)) => Left(ValidationError.Oops)

        case Alter(AlterTarget.Column(name, op)) =>
          (table.columns
            .foldLeft[Either[ValidationError, (List[Schema.Table.Column], Boolean)]](Right(Nil -> false)) {
              case (Left(err), _)                           => Left(err)
              case (Right(acc, true), c)                    => Right((c :: acc) -> true)
              case (Right(acc, false), c) if c.name == name => c.alter(op).map(c => (c :: acc) -> true)
              case (Right(acc, false), c)                   => Right((c :: acc) -> false)
            })
            .flatMap {
              case (_, false) =>
                Left(ValidationError.SchemaUpdate(Schema.UpdateError.ColumnDoesNotExist(table.name, name)))
              case (cols, true) =>
                Right(table.copy(columns = cols.reverse))
            }
      }
    }
  }

  extension (sch: Schema) {
    def alterTable(name: Schema.Relation.Name, op: Migration.Alter.Table.Operation): Either[ValidationError, Schema] =
      sch.tables.get(name) match {
        case None        => Left(ValidationError.SchemaUpdate(Schema.UpdateError.RealtionDoesNotExist(name)))
        case Some(table) => table.alter(sch, op).map(table => sch.copy(tables = sch.tables + (table.name -> table)))
      }

  }

  def evalToSchema: Algebra[Migration.MigrationF, Either[ValidationError, Schema]] = {
    case InitF                             => Right(Schema.empty)
    case CreateF(sch, table: Schema.Table) => sch.flatMap(_.withTable(table).left.map(ValidationError.SchemaUpdate(_)))

    case AlterF(sch, Alter.Target.Table(name, op)) => sch.flatMap(_.alterTable(name, op))

    // TODO: take the flag into account
    case DropF(sch, Migration.Drop.Target.Table(name, _)) =>
      sch.flatMap(_.withoutTable(name).left.map(ValidationError.SchemaUpdate(_)))
    case DropF(sch, Migration.Drop.Target.Tables(names, _)) =>
      names.foldLeft[Either[ValidationError, Schema]](sch) { (acc, name) =>
        acc.flatMap(_.withoutTable(name).left.map(ValidationError.SchemaUpdate(_)))
      }

    // Assume raw ops does not affect the schema
    case RawF(sch, _) => sch

    case _ => Left(ValidationError.Oops)
  }

  def evalToSql: Algebra[Migration.MigrationF, List[String]] = {
    case InitF                                        => Nil
    case CreateF(prev, table: Schema.Table)           => table.createQuery :: prev
    case AlterF(prev, alterTable: Alter.Target.Table) => alterTable.query :: prev

    case DropF(prev, dropTable: Migration.Drop.Target.Table)   => dropTable.query :: prev
    case DropF(prev, dropTables: Migration.Drop.Target.Tables) => dropTables.query :: prev

    case RawF(prev, sql) => sql :: prev
  }

  def evalToApply: Algebra[Migration.MigrationF, ZIO[Protocol, ApplyError, Unit]] = {

    def run(q: String) = Protocol.simpleQuery(q).runCollect.unit.mapError(ApplyError.RunQuery(q, _))

    {
      case InitF                                        => ZIO.succeed(())
      case CreateF(prev, table: Schema.Table)           => prev *> run(table.createQuery)
      case AlterF(prev, alterTable: Alter.Target.Table) => prev *> run(alterTable.query)

      case DropF(prev, dropTable: Migration.Drop.Target.Table)   => prev *> run(dropTable.query)
      case DropF(prev, dropTables: Migration.Drop.Target.Tables) => prev *> run(dropTables.query)

      case RawF(prev, sql) => prev *> run(sql)
    }
  }
}
