package zio.postgres.protocol

import zio.*
import zio.stream.*

trait Parser {
  def pipeline: ZPipeline[Any, Nothing, Byte, Parser.Packet]
}

object Parser {

  enum Packet {
    case Generic(`type`: Byte, payload: Chunk[Byte])
  }

  def pipeline: ZPipeline[Parser, Nothing, Byte, Packet] =
    ZPipeline.fromChannel(
      ZChannel.serviceWithChannel[Parser](_.pipeline.channel)
    )

  case class Live() extends Parser {
    override def pipeline: ZPipeline[Any, Nothing, Byte, Packet] =
      ZPipeline.fromPush(ZIO.succeed { in =>
        println(s"=============> $in")
        ZIO.succeed(Chunk.empty)
      })
  }

  def live: ULayer[Parser] = ZLayer.succeed(Live())
}
