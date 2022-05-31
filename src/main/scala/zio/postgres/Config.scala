package zio.postgres

case class Config(host: String, port: Int, database: String, user: String, password: String)
