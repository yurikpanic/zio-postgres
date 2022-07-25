package zio.postgres.ddl

sealed trait PrimaryKey[A]

sealed trait Table[A <: Product]

sealed trait Raw[Q <: String]

sealed trait Extensions[E <: Tuple]
