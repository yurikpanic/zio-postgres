package zio.postgres.protocol

import zio.*

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets.UTF_8

object Messages {

  def startupMessage(user: String, database: String): ByteBuffer =
    Gen.make(
      Field.Length,
      Field.Int32(196608),
      Field.String("user"),
      Field.String(user),
      Field.String("database"),
      Field.String(database),
      Field.String("application_name"),
      Field.String("zio-postgres"),
      Field.Byte(0) // parameters terminator
    )

}
