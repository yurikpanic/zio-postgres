package zio.postgres
package protocol

import zio.*
import zio.stream.*

import connection.*

import java.nio.ByteBuffer

trait Protocol {
  def simpleQuery(query: String): UIO[Unit]
}

object Protocol {

  case class Live(outQ: Queue[ByteBuffer]) extends Protocol {
    override def simpleQuery(query: String): UIO[Unit] = ZIO.unit
  }

  def live(outQ: Queue[ByteBuffer], in: ZStream[Any, Connection.Error, Packet]): URIO[Scope, Live] =
    for {
      _ <- in.runDrain.forkScoped // TODO
    } yield Live(outQ)

}
