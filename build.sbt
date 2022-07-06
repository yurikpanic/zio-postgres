import xerial.sbt.Sonatype._
import ReleaseTransformations._

val scala3Version = "3.1.3"

ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"

lazy val root = project
  .in(file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "zio-postgres",
    organization := "io.github.yurikpanic",
    scalaVersion := scala3Version,
    publishTo := sonatypePublishToBundle.value,
    publishMavenStyle := true,
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    sonatypeProjectHosting := Some(GitHubHosting("yurikpanic", "zio-postgres", "vishnevsky@gmail.com")),
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      runTest,
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      releaseStepCommand("publishSigned"),
      releaseStepCommand("sonatypeBundleRelease"),
      setNextVersion,
      commitNextVersion,
      pushChanges
    ),
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "0.7.29" % Test,
      "dev.zio" %% "zio" % "2.0.0",
      "dev.zio" %% "zio-streams" % "2.0.0",
      "dev.zio" %% "zio-prelude" % "1.0.0-RC15",
      "com.bolyartech.scram_sasl" % "scram_sasl" % "2.0.2"
    ),
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding",
      "UTF-8",
      "-feature"
    ),
    fork := true
  )
