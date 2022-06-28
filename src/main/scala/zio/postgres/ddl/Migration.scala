package zio.postgres
package ddl

import Schema.*

final case class Migration(
    ops: List[Migration.Operation]
)

object Migration {

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
          case SetNotNul
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

  enum Operation {
    case Create(what: Schema.Relation)
    case Alter(what: Migration.Alter.Target)
    case Drop(what: Migration.Drop.Target)
  }

}
