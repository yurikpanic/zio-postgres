package zio.postgres
package ddl

import zio.prelude.*

import Schema.*

type Migration = Fix[Migration.MigrationF]

object Migration {

  enum ValidationError extends Throwable {
    case SchemaUpdate(err: Schema.UpdateError)
    case Oops // TODO: this marks unimplemented parts - remove it once all is done
  }

  extension (m: Migration) {
    def toSchema: Either[ValidationError, Schema] = {
      import eval.*
      cata(MigrationEval.evalToSchema)(m)
    }
  }

  object Table {
    object Drop {
      enum Flag {
        case Restrict
        case Cascade
      }
    }
  }

  object Alter {

    enum Target {
      case Table(name: Relation.Name, op: Alter.Table.Operation)
    }

    object Table {
      enum Operation {
        case Add(what: Table.Add.Target)
        case Rename(what: Table.Rename.Target)
        case Drop(what: Table.Drop.Target)
        case Alter(what: Table.Alter.Target)
      }

      object Add {
        enum Target {
          case Column(name: String, dataType: Type, nullable: Boolean = true)
        }
      }

      object Rename {
        enum Target {
          case Column(name: String, to: String)
          case Constraint(name: String, to: String)
          case Table(to: String)
        }
      }

      object Drop {

        enum Target {
          case Column(name: String, flag: Option[Migration.Table.Drop.Flag] = None)
          case Constraint(name: String, flag: Option[Migration.Table.Drop.Flag] = None)
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

        enum Target {
          case Column(name: String, op: Operation)
        }
      }
    }
  }

  object Drop {
    enum Target {
      case Table(name: Relation.Name, flag: Option[Migration.Table.Drop.Flag] = None)
      case Tables(name: List[Relation.Name], flag: Option[Migration.Table.Drop.Flag] = None)
    }
  }

  sealed trait MigrationF[+R]

  case object InitF extends MigrationF[Nothing]
  final case class CreateF[R](was: R, what: Schema.Relation) extends MigrationF[R]
  final case class AlterF[R](was: R, what: Migration.Alter.Target) extends MigrationF[R]
  final case class DropF[R](was: R, what: Migration.Drop.Target) extends MigrationF[R]

  given Covariant[MigrationF] with {
    override def map[A, B](f: A => B): MigrationF[A] => MigrationF[B] = {
      case InitF              => InitF
      case CreateF(was, what) => CreateF(f(was), what)
      case AlterF(was, what)  => AlterF(f(was), what)
      case DropF(was, what)   => DropF(f(was), what)
    }
  }

}
