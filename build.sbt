val scala3Version = "3.1.2"

lazy val root = project
  .in(file("."))
  .settings(
    name := "zio-postgres",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "0.7.29" % Test,
      "dev.zio" %% "zio" % "2.0.0-RC6",
      "dev.zio" %% "zio-streams" % "2.0.0-RC6",
      "com.bolyartech.scram_sasl" % "scram_sasl" % "2.0.2"
    ),
    run / fork := true
  )
