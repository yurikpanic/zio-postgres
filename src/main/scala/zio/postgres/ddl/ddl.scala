package zio.postgres.ddl

sealed trait Config

sealed trait RenamedFrom[From <: String, To <: String] extends Config

sealed trait PrimaryKey[A]

object PrimaryKey {
  final case class Id[A](value: A) extends PrimaryKey[A]

  extension [A](pk: PrimaryKey[A]) {
    def value: A = pk match {
      case Id(x) => x
    }
  }
}

sealed trait Table[A <: Product]

sealed trait Hawing[T <: Table[?] | Hawing[?, ?], C <: Config]

sealed trait Raw[Q <: String]

sealed trait Extensions[E <: Tuple]

sealed trait Publication[Tables <: Tuple | String]

sealed trait ReplicationSlot[Plugin <: String]
