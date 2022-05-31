package zio.postgres.protocol

import zio._
import zio.stream._

trait Parser {
  def pipeline: ZPipeline[Any, Nothing, Byte, Parser.Packet]
}

object Parser {

  enum Packet {
    case Generic(`type`: Byte, payload: Chunk[Byte])
  }

  def pipeline: ZPipeline[Parser, Nothing, Byte, Parser.Packet] =
    ZPipeline.fromChannel(
      ZChannel.serviceWithChannel[Parser](_.pipeline.channel)
    )

  def live: ULayer[Parser] = ZLayer.succeed {
    new Parser {
      override def pipeline: ZPipeline[Any, Nothing, Byte, Parser.Packet] = ???
    }
  }
}
