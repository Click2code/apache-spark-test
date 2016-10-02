name := "restpi"

organization := "com.github.dnvriend"

scalaVersion := "2.11.8"

version := "1.0.0"

libraryDependencies ++= {
  Seq(
    "com.typesafe.akka" %% "akka-http-experimental" % "2.4.11",
    "com.typesafe.akka" %% "akka-http-spray-json-experimental" % "2.4.11"
  )
}