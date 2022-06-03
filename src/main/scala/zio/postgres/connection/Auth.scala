package zio.postgres
package connection

import zio.*
import zio.stream.*

import java.nio.ByteBuffer

import protocol.Packet

trait Auth { self =>
  def pipeline(q: Queue[ByteBuffer]): ZPipeline[Config, Auth.Error, Packet, Packet]

  def andThen(auth: Auth): Auth = new Auth {
    override def pipeline(q: Queue[ByteBuffer]) =
      self.pipeline(q) >>> auth.pipeline(q)
  }
}

object Auth {

  enum Error {
    case NotSupported(kind: Packet.AuthRequest.Kind)
  }

  def pipeline(q: Queue[ByteBuffer]): ZPipeline[Auth & Config, Error, Packet, Packet] =
    ZPipeline.fromChannel(
      ZChannel.serviceWithChannel[Auth](_.pipeline(q).channel)
    )

  object Plain extends Auth {
    override def pipeline(q: Queue[ByteBuffer]) =
      ZPipeline.fromChannel {
        ZChannel.identity.mapOutZIO { ps =>
          ZIO.foldLeft(ps)(Chunk.empty[Packet]) {
            case (acc, p @ Packet.AuthRequest(Packet.AuthRequest.Kind.CleartextPassword)) =>
              for {
                cfg <- ZIO.service[Config]
                _ <- q.offer(Packet.passwordMessage(cfg.password))
              } yield acc
            case (acc, other) =>
              ZIO.succeed(acc :+ other)
          }
        }
      }
  }

  object Md5 extends Auth {
    override def pipeline(q: Queue[ByteBuffer]) = ZPipeline.identity // TODO
  }

  case object Fail extends Auth {
    override def pipeline(q: Queue[ByteBuffer]) =
      ZPipeline.mapZIO {
        case Packet.AuthRequest(kind) if kind != Packet.AuthRequest.Kind.Ok =>
          ZIO.fail(Error.NotSupported(kind))

        case p => ZIO.succeed(p)
      }
  }

  val live: ULayer[Auth] = ZLayer.succeed(Plain >>> Md5 >>> Fail)

  extension (a: Auth) {
    def >>>(b: Auth) = a.andThen(b)
  }
}
