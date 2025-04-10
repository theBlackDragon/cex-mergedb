import sbtsonar.SonarPlugin.autoImport.sonarProperties

organization := "be.lair"

name := "cex-mergedb"

version := "0.0.1-SNAPSHOT"

scalaVersion := "3.3.5"

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")

lazy val sonarSettings = Seq(
  sonarProperties ++= Map(
    "sonar.projectKey" -> "CEX_MERGEDB"
  ))

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.5.18",
  "org.xerial" % "sqlite-jdbc" % "3.49.1.0",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "org.scalatest" %% "scalatest-wordspec" % "3.2.19" % Test
)
