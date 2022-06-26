val scala3Version = "3.1.3"

lazy val root = project
  .in(file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "zio-postgres",
    organization := "iv",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "0.7.29" % Test,
      "dev.zio" %% "zio" % "2.0.0",
      "dev.zio" %% "zio-streams" % "2.0.0",
      "com.bolyartech.scram_sasl" % "scram_sasl" % "2.0.2"
    ),
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding",
      "UTF-8",
      "-feature",
      "-Yscala-release:3.0"
    ),
    fork := true
  )
