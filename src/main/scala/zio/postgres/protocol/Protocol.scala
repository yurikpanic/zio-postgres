package zio.postgres.protocol

import zio._

trait Protocol {
  def simpleQuery(query: String): UIO[Unit]
}
