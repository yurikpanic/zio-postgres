package zio.postgres
package connection

import com.bolyartech.scram_sasl.client.ScramSaslClientProcessor
import com.bolyartech.scram_sasl.client.ScramSha256SaslClientProcessor
import zio.*
import zio.stream.*

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import protocol.Packet

trait Auth { self =>
  def pipeline(q: Queue[ByteBuffer]): ZPipeline[Scope & Config, Auth.Error, Packet, Packet]

  def andThen(auth: Auth): Auth = new Auth {
    override def pipeline(q: Queue[ByteBuffer]) =
      self.pipeline(q) >>> auth.pipeline(q)
  }
}

object Auth {

  enum Error {
    case NotSupported(kind: Packet.AuthRequest.Kind)
    case SaslException(e: Throwable)
  }

  def pipeline(q: Queue[ByteBuffer]): ZPipeline[Auth & Scope & Config, Error, Packet, Packet] =
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

  object Sasl extends Auth {

    val supportedMechanism = "SCRAM-SHA-256"

    override def pipeline(q: Queue[ByteBuffer]) = ZPipeline.fromChannel {
      ZChannel
        .fromZIO {
          val msgQ = Queue.unbounded[String].flatMap { msgQ =>
            ZStream
              .fromQueue(msgQ)
              .mapAccum(true) {
                case (true, s)  => false -> Packet.saslInitialResponseMessage(supportedMechanism, s)
                case (false, s) => false -> Packet.saslResponseMessage(s)
              }
              .run(ZSink.fromQueue(q))
              .forkScoped
              .as(msgQ)
          }
          ZIO.runtime zip msgQ
        }
        .flatMap { case (rt, msgQ) =>
          val listener = new ScramSaslClientProcessor.Listener {
            override def onSuccess(): Unit = ()
            override def onFailure(): Unit = ()
          }

          val sender = new ScramSaslClientProcessor.Sender {
            override def sendMessage(msg: String): Unit = rt.unsafeRunAsync(msgQ.offer(msg))
          }

          ZChannel.fromZIO {
            ZIO
              .attempt(new ScramSha256SaslClientProcessor(listener, sender))
              .mapError(Error.SaslException(_))
          }
        }
        .flatMap { client =>
          ZChannel.identity
            .mapOutZIO { ps =>
              def saslContinue(data: Array[Byte]) = ZIO
                .attempt(client.onMessage(new String(data, UTF_8)))
                .mapError(Error.SaslException(_))

              ZIO.foldLeft(ps)(Chunk.empty[Packet]) {
                case (acc, Packet.AuthRequest(Packet.AuthRequest.Kind.SASL(methods)))
                    if methods.exists(_.compareToIgnoreCase(supportedMechanism) == 0) =>
                  for {
                    cfg <- ZIO.service[Config]
                    _ <- ZIO
                      .attempt(client.start(cfg.user, cfg.password))
                      .mapError(Error.SaslException(_))
                  } yield acc

                case (acc, Packet.AuthRequest(Packet.AuthRequest.Kind.SASLContinue(data))) =>
                  saslContinue(data).as(acc)

                case (acc, Packet.AuthRequest(Packet.AuthRequest.Kind.SASLFinal(data))) =>
                  saslContinue(data).as(acc)

                case (acc, other) => ZIO.succeed(acc :+ other)
              }
            }
        }

    }
  }

  case object Fail extends Auth {
    override def pipeline(q: Queue[ByteBuffer]) =
      ZPipeline.mapZIO {
        case Packet.AuthRequest(kind) if kind != Packet.AuthRequest.Kind.Ok =>
          ZIO.fail(Error.NotSupported(kind))

        case p => ZIO.succeed(p)
      }
  }

  val live: ULayer[Auth] = ZLayer.succeed(Plain >>> Sasl >>> Fail)

  extension (a: Auth) {
    def >>>(b: Auth) = a.andThen(b)
  }
}
