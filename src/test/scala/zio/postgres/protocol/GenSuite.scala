package zio.postgres.protocol

import zio.Chunk

class GenSuite extends munit.FunSuite {
  test("Generate initial mesage from fields") {
    val user = "someuser"
    val database = "somedatabase"
    val l = database.getBytes().length
    Gen.make(
      Field.Byte('p'),
      Field.Length,
      Field.Int32(l)
    )
    assertEquals(
      Chunk.fromByteBuffer(
        Gen
          .make(
            Field.Length,
            Field.Int32(196608),
            Field.String("user"),
            Field.String(user),
            Field.String("database"),
            Field.String(database)
          )
      ),
      Chunk[Byte](0, 0, 0, 44, 0, 3, 0, 0, 117, 115, 101, 114, 0, 115, 111, 109, 101, 117, 115, 101, 114, 0, 100, 97,
        116, 97, 98, 97, 115, 101, 0, 115, 111, 109, 101, 100, 97, 116, 97, 98, 97, 115, 101, 0)
    )
  }
}
