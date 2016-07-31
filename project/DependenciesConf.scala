package sbt

import sbt._
import sbt.Keys._

object DependenciesConf {
  final val AkkaVersion = "2.4.8"

  lazy val settings: Seq[Setting[_]] = Seq(
    libraryDependencies ++= {
      Seq(
        "org.apache.spark" %% "spark-core" % "2.0.0",
        "org.apache.spark" %% "spark-sql" % "2.0.0",
        "org.apache.spark" %% "spark-tags" % "2.0.0",
        "org.apache.spark" %% "spark-streaming" % "2.0.0",
        "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
        "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
        "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion,
        "ch.qos.logback" % "logback-classic" % "1.1.7",
        "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
        "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % Test,
        "org.scalatest" %% "scalatest" % "2.2.6" % Test
      )
    }
  )
}