package zio.postgres.protocol

import zio.*

trait Protocol {
  def simpleQuery(query: String): UIO[Unit]
}
