package zio.postgres.protocol

import zio.postgres

import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.util.Try

object Wal {
  enum Message {
    case PrimaryKeepAlive(walEnd: Long, clock: Long, needReply: Boolean)
    // TODO: need to decode the data
    case XLogData(startingPoint: Long, walEnd: Long, clock: Long, data: Array[Byte])
  }

  object Message {
    def parse(data: Array[Byte]): Either[Decoder.Error, Message] = {
      import Packet._

      val bb = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN)

      (bb.getByteSafe.toRight(Decoder.Error.ResultSetExhausted).flatMap {
        case 'k' =>
          (for {
            walEnd <- bb.getLongSafe
            clock <- bb.getLongSafe
            needReply <- bb.getByteSafe
          } yield PrimaryKeepAlive(walEnd, clock, needReply == 1)).toRight(Decoder.Error.ResultSetExhausted)

        case 'w' =>
          (for {
            startingPoint <- bb.getLongSafe
            walEnd <- bb.getLongSafe
            clock <- bb.getLongSafe
            dataLength = bb.limit() - bb.position()
            data = new Array[Byte](dataLength)
            _ <- Try(bb.get(data)).toOption
          } yield XLogData(startingPoint, walEnd, clock, data)).toRight(Decoder.Error.ResultSetExhausted)

        case other => Left(Decoder.Error.UnknownWalMessage(other))
      })
    }
  }

  given Decoder[Message] = new Decoder[Message] {
    override def decode: PartialFunction[Packet, Either[Decoder.Error, Message]] = { case Packet.CopyData(data) =>
      Message.parse(data)
    }

    // TODO: perhaps we should terminate on some errors and maybe some other conditions?
    override def isDone(p: Packet): Boolean = false
  }
}
