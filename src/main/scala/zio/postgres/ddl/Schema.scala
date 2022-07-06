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

    extension (table: Table) {
      def renamedColumn(name: String, to: String): Either[UpdateError, Table] =
        if (table.columns.exists(_.name == to)) Left(UpdateError.ColumnAlreadyExists(table.name, to))
        else {
          val (cols, updated) =
            table.columns.foldLeft[(List[Table.Column], Boolean)](Nil -> false) {
              case ((acc, true), c)                    => (c :: acc) -> true
              case ((acc, false), c) if c.name == name => (c.copy(name = to) :: acc) -> true
              case ((acc, false), c)                   => (c :: acc) -> false
            }
          if (updated) Right(table.copy(columns = cols.reverse))
          else Left(UpdateError.ColumnDoesNotExist(table.name, name))
        }

      def addedColumn(name: String, dataType: Type, nullable: Boolean = true): Either[UpdateError, Table] =
        if (table.columns.exists(_.name == name)) Left(UpdateError.ColumnAlreadyExists(table.name, name))
        else Right(table.copy(columns = table.columns :+ Table.Column(name, dataType, nullable = nullable)))

      def droppedColumn(name: String): Either[UpdateError, Table] = {
        val (cols, found) =
          table.columns.foldLeft[(List[Table.Column], Boolean)](Nil -> false) {
            case ((acc, true), c)                      => (c :: acc) -> true
            case ((acc, false), c) if (c.name == name) => acc -> true
            case ((acc, false), c)                     => (c :: acc) -> false
          }
        if (found) Right(table.copy(columns = cols.reverse))
        else Left(UpdateError.ColumnDoesNotExist(table.name, name))
      }

    }

  }

  object Relation {
    final case class Name(name: String, namespace: String = "public")
  }

  enum UpdateError {
    case RealtionAlreadyExists(name: Relation.Name)
    case RealtionDoesNotExist(name: Relation.Name)
    case ColumnAlreadyExists(name: Relation.Name, column: String)
    case ColumnDoesNotExist(name: Relation.Name, column: String)
  }

  extension (sch: Schema) {
    def withTable(table: Table): Either[UpdateError, Schema] = sch.tables.get(table.name) match {
      case None    => Right(sch.copy(tables = sch.tables + (table.name -> table)))
      case Some(_) => Left(UpdateError.RealtionAlreadyExists(table.name))
    }

    def withoutTable(name: Relation.Name): Either[UpdateError, Schema] = sch.tables.get(name) match {
      case Some(table) => Right(sch.copy(tables = sch.tables - name))
      case None        => Left(UpdateError.RealtionDoesNotExist(name))
    }
  }

}
