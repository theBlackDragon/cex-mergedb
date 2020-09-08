organization := "be.lair"

name := "cex-mergedb"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.13.3"

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.clapper" %% "grizzled-slf4j" % "1.3.4",
  "org.openjfx" % "javafx-controls" % "14",
  "org.xerial" % "sqlite-jdbc" % "3.32.3.2",
  "org.scalatest" %% "scalatest" % "3.2.2" % Test,
  "org.scalatest" %% "scalatest-wordspec" % "3.2.2" % Test
)
