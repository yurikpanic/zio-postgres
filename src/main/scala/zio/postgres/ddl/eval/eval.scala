package zio.postgres.ddl
package eval

import zio.prelude.*

type Algebra[F[_], A] = F[A] => A

def cata[F[+_], A](alg: Algebra[F, A])(using Covariant[F]): Fix[F] => A = { ex =>
  Fix.out[F].andThen(_.map(cata(alg))).andThen(alg)(ex)
}
