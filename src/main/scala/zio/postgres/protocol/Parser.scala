package zio.postgres.protocol

import zio.*
import zio.stream.*

import java.nio.ByteBuffer
import java.nio.ByteOrder

trait Parser {
  def pipeline: ZPipeline[Any, ZNothing, Byte, Parser.Packet]
}

object Parser {

  enum Packet {
    case Generic(`type`: Byte, payload: Chunk[Byte])
  }

  def pipeline: ZPipeline[Parser, ZNothing, Byte, Packet] =
    ZPipeline.fromChannel(
      ZChannel.serviceWithChannel[Parser](_.pipeline.channel)
    )

  enum State {
    case WaitTypeAndLength
    case WaitPacket(tpe: Byte, length: Int)
  }

  case class Live() extends Parser {
    override def pipeline: ZPipeline[Any, ZNothing, Byte, Packet] =
      ZPipeline.suspend {

        type DecodingChannel = ZChannel[Any, ZNothing, Chunk[Byte], Any, ZNothing, Chunk[Packet], Any]

        def decode(buffer: Chunk[Byte], state: State): DecodingChannel = ZChannel.readWith(
          received => {
            val data = buffer ++ received

            state match {
              case State.WaitTypeAndLength =>
                if (data.length >= 5) {
                  val (tpe, lRest) = data.splitAt(1)
                  val (lengthChunk, rest) = lRest.splitAt(4)

                  val length = ByteBuffer.wrap(lengthChunk.toArray).order(ByteOrder.BIG_ENDIAN).getInt
                  ZChannel.write(rest) >>> decode(Chunk.empty, State.WaitPacket(tpe(0), length - 4))
                } else {
                  decode(data, state)
                }
              case State.WaitPacket(tpe, length) =>
                if (data.length >= length) {
                  val (payload, rest) = data.splitAt(length)

                  val in = ZChannel.write(rest) >>> decode(Chunk.empty, State.WaitTypeAndLength)
                  val out = ZChannel.write(Chunk(Packet.Generic(tpe, payload)))

                  out *> in
                } else {
                  decode(data, state)
                }
            }
          },
          error = ZChannel.fail(_),
          done = _ => ZChannel.unit
        )

        new ZPipeline(decode(Chunk.empty, State.WaitTypeAndLength))
      }
  }

  def live: ULayer[Parser] = ZLayer.succeed(Live())

}
