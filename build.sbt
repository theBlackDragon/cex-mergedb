organization := "be.lair"

name := "cex-mergedb"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.13.8"

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.11",
  "org.clapper" %% "grizzled-slf4j" % "1.3.4",
  "org.xerial" % "sqlite-jdbc" % "3.36.0.3",
  "org.scalatest" %% "scalatest" % "3.2.13" % Test,
  "org.scalatest" %% "scalatest-wordspec" % "3.2.13" % Test
)
