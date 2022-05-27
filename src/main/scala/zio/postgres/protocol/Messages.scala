package zio.postgres.protocol

import zio._

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets.UTF_8

object Messages {

  def startupMessage(user: String, database: String): Chunk[Byte] = {

    val userBytes = user.getBytes(UTF_8)
    val databaseBytes = user.getBytes(UTF_8)

    val length = 4 + 4 + 5 + userBytes.length + 1 + 9 + databaseBytes.length + 1

    Chunk.fromByteBuffer(
      ByteBuffer
        .allocate(length)
        .order(ByteOrder.BIG_ENDIAN)
        .putInt(length)
        .putInt(196608)
        .put("user".getBytes(UTF_8))
        .put(0: Byte)
        .put(userBytes)
        .put(0: Byte)
        .put("database".getBytes(UTF_8))
        .put(0: Byte)
        .put(databaseBytes)
        .put(0: Byte)
    )
  }
}
