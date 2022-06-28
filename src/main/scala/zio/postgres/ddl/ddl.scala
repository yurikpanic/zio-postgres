package zio.postgres
package ddl

import scala.annotation.experimental
import scala.quoted.*

import zio.*

import protocol.Protocol

inline def migration(inline ops: Migration.Operation*): Migration = Migration(ops.toList)

inline def createTable(
    inline name: Schema.Relation.Name,
    inline columns: List[Schema.Table.Column],
    inline constrains: List[Schema.Table.Constraint] = Nil,
    inline ifNotExists: Boolean = false,
    inline kind: Schema.Table.Kind = Schema.Table.Kind.Ordinary
): Migration.Operation =
  Migration.Operation.Create(Schema.Table(name, columns, constrains, ifNotExists, kind))

inline def column(
    inline name: String,
    inline dataType: ddl.Type,
    inline primaryKey: Boolean = false,
    inline nullable: Boolean = true
) =
  Schema.Table.Column(name, dataType, primaryKey, nullable)

inline def alterTable(
    inline name: Schema.Relation.Name,
    inline op: Migration.Alter.Table.Operation
): Migration.Operation = Migration.Operation.Alter(Migration.Alter.Target.Table(name, op))

inline def addColumn(
    inline name: String,
    inline dataType: ddl.Type,
    inline nullable: Boolean = true
): Migration.Alter.Table.Operation =
  Migration.Alter.Table.Operation.Add(Migration.Alter.Table.Add.Target.Column(name, dataType, nullable))

inline def renameColumn(
    inline name: String,
    inline to: String
): Migration.Alter.Table.Operation =
  Migration.Alter.Table.Operation.Rename(Migration.Alter.Table.Rename.Target.Column(name, to))

extension (s: String) {
  inline def in(ns: String): Schema.Relation.Name = Schema.Relation.Name(s, ns)
  inline def public: Schema.Relation.Name = Schema.Relation.Name(s)
}
