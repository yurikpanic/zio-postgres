package zio.postgres.ddl.eval

final case class Fix[F[_]](unfix: F[Fix[F]])

object Fix {
  def in[F[_]]: F[Fix[F]] => Fix[F] = ff => new Fix[F](ff)
  def out[F[_]]: Fix[F] => F[Fix[F]] = f => f.unfix
}
