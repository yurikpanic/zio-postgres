package zio.postgres
package connection

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import zio.*
import zio.stream.*

trait Socket {
  def connect(host: String, port: Int): ZIO[
    Scope,
    IOException,
    (
        ZSink[Any, IOException, ByteBuffer, ByteBuffer, Unit],
        ZStream[Any, IOException, Byte]
    )
  ]
}

object Socket {

  def connect(host: String, port: Int): ZIO[
    Scope & Socket,
    IOException,
    (
        ZSink[Any, IOException, ByteBuffer, ByteBuffer, Unit],
        ZStream[Any, IOException, Byte]
    )
  ] = ZIO.serviceWithZIO[Socket](_.connect(host, port))

  object Tcp extends Socket {
    override def connect(host: String, port: Int): ZIO[
      Scope,
      IOException,
      (ZSink[Any, IOException, ByteBuffer, ByteBuffer, Unit], ZStream[Any, IOException, Byte])
    ] = for {
      channel <- ZIO
        .fromAutoCloseable(
          ZIO.attemptBlockingIO(
            SocketChannel.open(new InetSocketAddress(host, port))
          )
        )
      sink: ZSink[Any, IOException, ByteBuffer, ByteBuffer, Unit] = ZSink.fromPush(ZIO.succeed {
        case Some(in) =>
          ZIO
            .foreach(in) { bb =>
              ZIO.succeedBlocking(channel.write(bb))
            }
            .as(Chunk.empty)

        case None => ZIO.unit
      })

      bb = ByteBuffer.allocate(512)
      stream = ZStream.repeatZIOChunk(
        for {
          _ <- ZIO.succeed(bb.clear)
          read <- ZIO.attemptBlockingIO(channel.read(bb))
          _ <- ZIO.succeed(bb.limit(read))
          _ <- ZIO.succeed(bb.rewind)
        } yield Chunk.fromByteBuffer(bb)
      )
    } yield sink -> stream
  }

  val tcp = ZLayer.succeed(Tcp)
}
