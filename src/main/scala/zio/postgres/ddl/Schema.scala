package zio.postgres.ddl

transparent trait AnySchema {

  type Column

  sealed trait Relation

  object Relation {
    final case class Name(name: String, namespace: String = "public")

    extension (n: Name) {
      def toSql: String = s"${n.namespace}.${n.name}"
    }
  }

  extension (s: String) {
    inline def in(ns: String): Relation.Name = Relation.Name(s, ns)
    inline def public: Relation.Name = Relation.Name(s)
  }

}

object EmptySchema extends AnySchema

trait Schema[S <: AnySchema](val previous: S) extends AnySchema {

  type Column = ColumnDef

  case class ColumnDef(
      name: String,
      dataType: Type,
      origin: ColumnDef.Origin = ColumnDef.Origin.Create,
      primaryKey: Boolean = false,
      nullable: Boolean = true
  )

  object ColumnDef {
    enum Origin {
      case Create
      case Retain
      case Add
      case Rename(from: previous.Column)
    }
  }

  final case class Table(
      name: Relation.Name,
      columns: List[Column]
  ) extends Relation

  def tables: List[Table]
}

object Schema {

  // sealed trait Relation

  // object Table {
  //   enum Kind {
  //     case Ordinary
  //     case Temporary
  //     case Unlogged
  //   }

  //   extension (k: Kind) {
  //     def toSql: String = k match {
  //       case Kind.Ordinary  => ""
  //       case Kind.Temporary => " TEMPORARY "
  //       case Kind.Unlogged  => " UNLOGGED "
  //     }
  //   }

  //   final case class Column(
  //       name: String,
  //       dataType: Type,
  //       origin: Column.Origin,
  //       primaryKey: Boolean = false,
  //       nullable: Boolean = true
  //   )

  //   object Column {
  //     enum Origin {
  //       case Create
  //       case Retain
  //       case Add
  //       case Rename(from: Column)
  //     }
  //   }

  //   extension (c: Column) {
  //     def toSql: String =
  //       s"${c.name} ${c.dataType.toSql}${if (c.nullable) "" else " not null "}${
  //           if (c.primaryKey) " primary key " else ""
  //         }"
  //   }

  //   sealed trait Constraint {
  //     def name: Option[String]
  //   }

  //   object Constraint {
  //     sealed trait Unnamed extends Constraint {
  //       override val name: Option[String] = None
  //     }
  //     final case class Unique(columns: List[String]) extends Unnamed
  //     final case class PrimaryKey(columns: List[String]) extends Unnamed
  //     final case class ForeignKey(columns: List[String], refTable: String, refColumns: List[String]) extends Unnamed

  //     final case class Named(_name: String, c: Unnamed) extends Constraint {
  //       override def name: Option[String] = Some(_name)
  //     }

  //     extension (c: Constraint) {
  //       def toSql: String = c match {
  //         case Unique(cols)     => s" unique (${cols.mkString(", ")}) "
  //         case PrimaryKey(cols) => s" primary key (${cols.mkString(", ")}) "
  //         case ForeignKey(cols, refTable, refCols) =>
  //           s" foreign key (${cols.mkString(", ")}) references $refTable (${refCols.mkString(", ")}) "
  //         case Named(name, constr) => s" constraint $name ${constr.toSql} "
  //       }
  //     }
  //   }

  // extension (table: Table) {
  // def renamedColumn(name: String, to: String): Either[UpdateError, Table] =
  //   if (table.columns.exists(_.name == to)) Left(UpdateError.ColumnAlreadyExists(table.name, to))
  //   else {
  //     val (cols, updated) =
  //       table.columns.foldLeft[(List[Table.Column], Boolean)](Nil -> false) {
  //         case ((acc, true), c)                    => (c :: acc) -> true
  //         case ((acc, false), c) if c.name == name => (c.copy(name = to) :: acc) -> true
  //         case ((acc, false), c)                   => (c :: acc) -> false
  //       }
  //     if (updated) Right(table.copy(columns = cols.reverse))
  //     else Left(UpdateError.ColumnDoesNotExist(table.name, name))
  //   }

  // def addedColumn(name: String, dataType: Type, nullable: Boolean = true): Either[UpdateError, Table] =
  //   if (table.columns.exists(_.name == name)) Left(UpdateError.ColumnAlreadyExists(table.name, name))
  //   else Right(table.copy(columns = table.columns :+ Table.Column(name, dataType, nullable = nullable)))

  // def droppedColumn(name: String): Either[UpdateError, Table] = {
  //   val (cols, found) =
  //     table.columns.foldLeft[(List[Table.Column], Boolean)](Nil -> false) {
  //       case ((acc, true), c)                      => (c :: acc) -> true
  //       case ((acc, false), c) if (c.name == name) => acc -> true
  //       case ((acc, false), c)                     => (c :: acc) -> false
  //     }
  //   if (found) Right(table.copy(columns = cols.reverse))
  //   else Left(UpdateError.ColumnDoesNotExist(table.name, name))
  // }

  //   def createQuery: String =
  //     s"""
  //       create ${table.kind.toSql} table ${table.name.toSql} (${(table.columns.map(_.toSql) :::
  //         table.constrains.map(_.toSql)).mkString(", ")})
  //     """
  // }

  // }

  // enum UpdateError extends Throwable {
  //   case RealtionAlreadyExists(name: Relation.Name)
  //   case RealtionDoesNotExist(name: Relation.Name)
  //   case ColumnAlreadyExists(name: Relation.Name, column: String)
  //   case ColumnDoesNotExist(name: Relation.Name, column: String)
  // }

  // extension (sch: Schema) {
  //   def withTable(table: Table): Either[UpdateError, Schema] = sch.tables.get(table.name) match {
  //     case None    => Right(sch.copy(tables = sch.tables + (table.name -> table)))
  //     case Some(_) => Left(UpdateError.RealtionAlreadyExists(table.name))
  //   }

  //   def withoutTable(name: Relation.Name): Either[UpdateError, Schema] = sch.tables.get(name) match {
  //     case Some(table) => Right(sch.copy(tables = sch.tables - name))
  //     case None        => Left(UpdateError.RealtionDoesNotExist(name))
  //   }
  // }

}
