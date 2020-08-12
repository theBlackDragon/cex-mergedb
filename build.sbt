organization := "be.lair"

name := "cex-mergedb"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.13.1"

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.clapper" %% "grizzled-slf4j" % "1.3.4",
  "org.xerial" % "sqlite-jdbc" % "3.28.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)
