package zio.postgres.ddl

enum Type {
  case Int
  case Long
  case Text
}

object Type {
  extension (t: Type) {
    def toSql: String = t match {
      case Int  => " integer "
      case Long => " bigint "
      case Text => " text "
    }
  }
}
