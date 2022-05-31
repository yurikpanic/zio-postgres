package zio.postgres
package connection

import zio.*
import zio.stream.*

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

trait Socket {
  def sink: ZSink[Any, IOException, ByteBuffer, ByteBuffer, Unit]
  def stream: ZStream[Any, IOException, Byte]
}

object Socket {

  def sink: ZSink[Socket, IOException, ByteBuffer, ByteBuffer, Unit] = ZSink.serviceWithSink(_.sink)
  def stream: ZStream[Socket, IOException, Byte] = ZStream.serviceWithStream(_.stream)

  val tcp: ZLayer[Scope & Config, IOException, Socket] = ZLayer {
    ZIO.serviceWithZIO[Config] { cfg =>
      ZIO
        .fromAutoCloseable(
          ZIO.attemptBlockingIO(
            SocketChannel.open(new InetSocketAddress(cfg.host, cfg.port))
          )
        )
        .map { channel =>
          val _sink: ZSink[Any, IOException, ByteBuffer, ByteBuffer, Unit] = ZSink.fromPush(ZIO.succeed {
            case Some(in) =>
              ZIO
                .foreach(in) { bb =>
                  ZIO.succeedBlocking(channel.write(bb))
                }
                .as(Chunk.empty)

            case None => ZIO.unit
          })

          val _stream = ZStream.fromPull(
            for {
              bb <- ZIO.succeed(ByteBuffer.allocate(512))
            } yield {
              for {
                _ <- ZIO.succeed(bb.clear)
                read <- ZIO.attemptBlockingIO(channel.read(bb))
                _ <- ZIO.succeed(bb.limit(read))
                _ <- ZIO.succeed(bb.rewind)
              } yield Chunk.fromByteBuffer(bb)
            }.mapError(Option(_))
          )

          new Socket {
            override def sink: ZSink[Any, IOException, ByteBuffer, ByteBuffer, Unit] = _sink
            override def stream: ZStream[Any, IOException, Byte] = _stream

          }
        }
    }
  }
}
