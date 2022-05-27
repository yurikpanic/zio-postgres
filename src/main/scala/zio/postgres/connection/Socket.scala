package zio.postgres
package connection

import zio.*
import zio.stream.*

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.channels.SocketChannel

trait Socket {
  def sink: ZSink[Any, IOException, Byte, Byte, Long]
  def stream: ZStream[Any, IOException, Byte]
}

object Socket {

  def sink: ZSink[Socket, IOException, Byte, Byte, Long] = ZSink.serviceWithSink(_.sink)
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
          val _sink = ZSink.fromOutputStream(channel.socket.getOutputStream)
          val _stream = ZStream.fromInputStream(channel.socket.getInputStream, 1024)

          new Socket {
            override def sink: ZSink[Any, IOException, Byte, Byte, Long] = _sink
            override def stream: ZStream[Any, IOException, Byte] = _stream

          }
        }
    }
  }
}
