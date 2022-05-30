package zio.postgres.protocol

import zio._

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets.UTF_8

object Messages {

  def startupMessage(user: String, database: String): Chunk[Byte] =
    Gen.make(
      Field.Length,
      Field.Int32(196608),
      Field.String("user"),
      Field.String(user),
      Field.String("database"),
      Field.String(database)
    )

}
