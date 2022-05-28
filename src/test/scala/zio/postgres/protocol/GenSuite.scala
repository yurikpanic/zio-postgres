package zio.postgres.protocol

class GenSuite extends munit.FunSuite {
  test("Generate messages from fields") {
    val user = "someuser"
    val password = "somepassword"
    Gen.make(
      Field.Length,
      Field.Int32(196608),
      Field.String("user"),
      Field.String(user),
      Field.String("password"),
      Field.String(password)
    )
  }
}
