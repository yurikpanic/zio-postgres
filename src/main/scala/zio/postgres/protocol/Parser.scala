package zio.postgres.protocol

import java.nio.ByteBuffer
import java.nio.ByteOrder

import scala.util.chaining.*

import zio.*
import zio.stream.*

trait Parser {
  def pipeline: ZPipeline[Any, Packet.ParseError, Byte, Packet]
}

object Parser {

  def pipeline: ZPipeline[Parser, Packet.ParseError, Byte, Packet] =
    ZPipeline.fromChannel(
      ZChannel.serviceWithChannel[Parser](_.pipeline.channel)
    )

  enum State {
    case WaitTypeAndLength
    case WaitPacket(tpe: Byte, length: Int)
  }

  object Live extends Parser {
    override def pipeline: ZPipeline[Any, Packet.ParseError, Byte, Packet] =
      ZPipeline.suspend {

        def decode(
            buffer: Chunk[Byte],
            state: State
        ): ZChannel[Any, ZNothing, Chunk[Byte], Any, Packet.ParseError, Chunk[Packet], Any] =
          ZChannel.readWith(
            received => {
              val data = buffer ++ received

              state match {
                case State.WaitTypeAndLength =>
                  if (data.length >= 5) {
                    val (tpe, lRest) = data.splitAt(1)
                    val (lengthChunk, rest) = lRest.splitAt(4)

                    val length = ByteBuffer.wrap(lengthChunk.toArray).order(ByteOrder.BIG_ENDIAN).getInt

                    (ZChannel.write(rest) *> ZChannel.identity[ZNothing, Chunk[Byte], Any]) >>>
                      decode(Chunk.empty, State.WaitPacket(tpe(0), length - 4))
                  } else {
                    decode(data, state)
                  }
                case State.WaitPacket(tpe, length) =>
                  if (data.length >= length) {
                    val (payload, rest) = data.splitAt(length)

                    Packet.parse(tpe, payload).fold(ZChannel.fail(_), Chunk(_).pipe(ZChannel.write(_))) *>
                      ((ZChannel.write(rest) *> ZChannel.identity[ZNothing, Chunk[Byte], Any]) >>>
                        decode(Chunk.empty, State.WaitTypeAndLength))
                  } else {
                    decode(data, state)
                  }
              }
            },
            error = ZChannel.fail(_),
            done = _ => ZChannel.unit
          )

        ZPipeline.fromChannel(decode(Chunk.empty, State.WaitTypeAndLength))
      }
  }

  def live: ULayer[Parser] = ZLayer.succeed(Live)

}
