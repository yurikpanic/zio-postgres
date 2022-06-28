package zio.postgres.ddl

final case class Schema(tables: Map[Schema.Relation.Name, Schema.Table])

object Schema {
  def empty = Schema(tables = Map.empty)

  sealed trait Relation

  final case class Table(
      name: Relation.Name,
      columns: List[Table.Column],
      constrains: List[Table.Constraint],
      ifNotExists: Boolean,
      kind: Table.Kind
  ) extends Relation

  object Table {
    enum Kind {
      case Ordinary
      case Temporary
      case Unlogged
    }

    final case class Column(name: String, dataType: Type, primaryKey: Boolean = false, nullable: Boolean = true)

    sealed trait Constraint {
      def name: Option[String]
    }

    object Constraint {
      sealed trait Unnamed extends Constraint {
        override val name: Option[String] = None
      }
      final case class Unique(columns: List[String]) extends Unnamed
      final case class PrimaryKey(columns: List[String]) extends Unnamed
      final case class ForeignKey(columns: List[String], refTable: String, refColumns: List[String]) extends Unnamed

      final case class Named(_name: String, c: Unnamed) extends Constraint {
        override def name: Option[String] = Some(_name)
      }
    }

  }

  object Relation {
    final case class Name(name: String, namespace: String = "public")
  }
}
