package zio.postgres
package ddl

import zio.*
import zio.prelude.*

import protocol.Protocol
import Schema.*

type Migration = Fix[Migration.MigrationF]

object Migration {

  enum ValidationError extends Throwable {
    case SchemaUpdate(err: Schema.UpdateError)
    case Oops // TODO: this marks unimplemented parts - remove it once all is done
  }

  enum ApplyError extends Throwable {
    case RunQuery(query: String, err: Protocol.Error)
  }

  extension (m: Migration) {
    def toSchema: Either[ValidationError, Schema] = {
      import eval.*
      cata(MigrationEval.evalToSchema)(m)
    }

    def toApply: ZIO[Protocol, ApplyError, Unit] = {
      import eval.*
      cata(MigrationEval.evalToApply)(m)
    }

    def toSql: List[String] = {
      import eval.*
      cata(MigrationEval.evalToSql)(m).reverse
    }
  }

  object Table {
    object Drop {
      enum Flag {
        case Restrict
        case Cascade
      }

      extension (f: Flag) {
        def toSql: String = f match {
          case Flag.Restrict => " restrict "
          case Flag.Cascade  => " cascade "
        }
      }
    }
  }

  object Alter {

    enum Target {
      case Table(name: Relation.Name, op: Alter.Table.Operation)
    }

    object Target {
      object Table {
        extension (alterTable: Table) {
          def query: String = s" alter table ${alterTable.name.toSql} ${alterTable.op.toSql} "
        }
      }
    }

    object Table {
      enum Operation {
        case Add(what: Table.Add.Target)
        case Rename(what: Table.Rename.Target)
        case Drop(what: Table.Drop.Target)
        case Alter(what: Table.Alter.Target)
      }

      extension (op: Operation) {
        def toSql: String = op match {
          case Operation.Add(what)    => what.toSql
          case Operation.Rename(what) => what.toSql
          case Operation.Drop(what)   => what.toSql
          case Operation.Alter(what)  => what.toSql
        }
      }

      object Add {
        enum Target {
          case Column(name: String, dataType: Type, nullable: Boolean = true)
        }

        extension (t: Target) {
          def toSql: String = t match {
            case Target.Column(name, dataType, nullable) =>
              s" add column $name ${dataType.toSql}${if (nullable) "" else " not null"} "
          }
        }
      }

      object Rename {
        enum Target {
          case Column(name: String, to: String)
          case Constraint(name: String, to: String)
          case Table(to: String)
        }

        extension (t: Target) {
          def toSql: String = t match {
            case Target.Column(name, to)     => s" rename column $name to $to "
            case Target.Constraint(name, to) => s" rename constraint $name to $to "
            case Target.Table(to)            => s" rename to $to "
          }
        }
      }

      object Drop {

        enum Target {
          case Column(name: String, flag: Option[Migration.Table.Drop.Flag] = None)
          case Constraint(name: String, flag: Option[Migration.Table.Drop.Flag] = None)
        }

        extension (t: Target) {
          def toSql: String = t match {
            case Target.Column(name, flag)     => s" drop column $name${flag.fold("")(_.toSql)} "
            case Target.Constraint(name, flag) => s" drop constraint $name${flag.fold("")(_.toSql)} "
          }
        }
      }

      object Alter {
        enum Operation {
          case SetType(tpe: Type)
          case SetDefault(expr: String) // TODO: need some SQL dsl here instead of a raw string
          case DropDefault
          case SetNotNull
          case DropNotNull
        }

        extension (op: Operation) {
          def toSql: String = op match {
            case Operation.SetType(tpe)     => s" type ${tpe.toSql} "
            case Operation.SetDefault(expr) => s" set default $expr "
            case Operation.DropDefault      => " drop default "
            case Operation.SetNotNull       => " set not null "
            case Operation.DropNotNull      => " drop not null "
          }
        }

        enum Target {
          case Column(name: String, op: Operation)
        }

        extension (t: Target) {
          def toSql: String = t match {
            case Target.Column(name, op) => s" alter column $name ${op.toSql} "
          }
        }
      }
    }
  }

  object Drop {
    enum Target {
      case Table(name: Relation.Name, flag: Option[Migration.Table.Drop.Flag] = None)
      case Tables(names: List[Relation.Name], flag: Option[Migration.Table.Drop.Flag] = None)
    }

    object Target {
      object Table {
        extension (dropTable: Table) {
          def query: String = s"drop table ${dropTable.name.toSql}"
        }
      }

      object Tables {
        extension (dropTables: Tables) {
          def query: String = dropTables.names.map(Table(_, dropTables.flag).query).mkString("; ")
        }
      }
    }
  }

  sealed trait MigrationF[+R]

  case object InitF extends MigrationF[Nothing]
  final case class CreateF[R](was: R, what: Schema.Relation) extends MigrationF[R]
  final case class AlterF[R](was: R, what: Migration.Alter.Target) extends MigrationF[R]
  final case class DropF[R](was: R, what: Migration.Drop.Target) extends MigrationF[R]
  final case class RawF[R](was: R, what: String) extends MigrationF[R]

  given Covariant[MigrationF] with {
    override def map[A, B](f: A => B): MigrationF[A] => MigrationF[B] = {
      case InitF              => InitF
      case CreateF(was, what) => CreateF(f(was), what)
      case AlterF(was, what)  => AlterF(f(was), what)
      case DropF(was, what)   => DropF(f(was), what)
      case RawF(was, what)    => RawF(f(was), what)
    }
  }

}
