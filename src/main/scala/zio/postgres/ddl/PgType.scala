package zio.postgres.ddl

trait PgType[A] {
  def renderCtor: String
}

object PgType {

  def apply[A](using PgType[A]): PgType[A] = summon[PgType[A]]

  given PgType[Int] = new {
    def renderCtor = "integer NOT NULL"
  }

  given PgType[String] = new {
    def renderCtor: String = "text NOT NULL"
  }

  given [A](using PgType[A]): PgType[Option[A]] = {

    val NN = "\\s+[nN][oO][tT]\\s+[nN][uU][lL][lL]\\s*$".r

    new {
      def renderCtor = NN.replaceAllIn(PgType[A].renderCtor, "")
    }
  }

  given [A](using PgType[A]): PgType[PrimaryKey[A]] = {

    new {
      def renderCtor = PgType[A].renderCtor + " PRIMARY KEY"
    }
  }

}
