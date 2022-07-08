package zio.postgres
package ddl

import scala.annotation.experimental
import scala.quoted.*

import zio.*

import protocol.Protocol

inline def migration(m: RunQuery => RunQuery) = m(Fix(Migration.InitF))

inline def createTable(
    inline name: Schema.Relation.Name,
    inline columns: List[Schema.Table.Column],
    inline constrains: List[Schema.Table.Constraint] = Nil,
    inline kind: Schema.Table.Kind = Schema.Table.Kind.Ordinary
): RunQuery => RunQuery = { (prev: RunQuery) =>
  Migration.CreateF[RunQuery](prev, Schema.Table(name, columns, constrains, kind))
}.andThen(Fix(_))

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
): RunQuery => RunQuery = { (prev: RunQuery) =>
  Migration.AlterF(prev, Migration.Alter.Target.Table(name, op))
}.andThen(Fix(_))

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

inline def raw(sql: String): RunQuery => RunQuery = { (prev: RunQuery) =>
  Migration.RawF[RunQuery](prev, sql)
}.andThen(Fix(_))

extension (s: String) {
  inline def in(ns: String): Schema.Relation.Name = Schema.Relation.Name(s, ns)
  inline def public: Schema.Relation.Name = Schema.Relation.Name(s)
}
