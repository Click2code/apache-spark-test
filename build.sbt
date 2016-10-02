name := "apache-spark-test"

organization := "com.github.dnvriend"

version := "1.0.0"

scalaVersion := "2.11.8"

fork in Test := true

parallelExecution in Test := false

licenses += ("Apache-2.0", url("http://opensource.org/licenses/apache2.0.php"))

resolvers += Resolver.typesafeRepo("releases")

resolvers += Resolver.jcenterRepo

lazy val jobs = (project in file("jobs"))
  .settings(SbtHeaderConf.settings ++ ScalariformConf.settings ++ commonSettings)
  .dependsOn(datasources)
  .enablePlugins(AutomateHeaderPlugin)

lazy val datasources = (project in file("datasources"))
  .settings(SbtHeaderConf.settings ++ ScalariformConf.settings ++ commonSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val helloworld = (project in file("helloworld"))
  .settings(SbtHeaderConf.settings ++ ScalariformConf.settings ++ commonSettings)
  .dependsOn(jobs)
  .enablePlugins(AutomateHeaderPlugin)

lazy val restpi = (project in file("restpi"))
  .settings(SbtHeaderConf.settings ++ ScalariformConf.settings ++ commonSettings)
  .dependsOn(jobs)
  .enablePlugins(AutomateHeaderPlugin)

lazy val commonSettings = Seq(
  parallelExecution in Test := false,
  fork in Test := true,
  libraryDependencies ++= deps
)

lazy val deps = { 
  val AkkaVersion = "2.4.11"
  val SparkVersion = "2.0.0"
  val slickVersion = "3.1.1"
  val hikariCPVersion = "2.5.1"
  Seq(
      "org.scala-lang" % "scala-reflect" % "2.11.8",
      "org.scalaz" %% "scalaz-core" % "7.2.6",
      "org.apache.spark" %% "spark-core" % SparkVersion,
      "org.apache.spark" %% "spark-sql" % SparkVersion,
      "org.apache.spark" %% "spark-mllib" % SparkVersion,
      "com.github.fommil.netlib" % "all" % "1.1.2",
      "org.apache.spark" %% "spark-tags" % SparkVersion,
      "org.apache.spark" %% "spark-streaming" % SparkVersion,
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % SparkVersion,
      "org.apache.bahir" %% "spark-streaming-twitter" % SparkVersion,
      "org.apache.bahir" %% "spark-streaming-akka" % SparkVersion,
      "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-M3",
//      "com.databricks" %% "spark-csv" % "1.4.0", // not necessary for spark v2.0.0
      "com.databricks" %% "spark-xml" % "0.4.0",
      "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
      "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
      "com.typesafe.akka" %% "akka-http-spray-json-experimental" % AkkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion,
      "ch.qos.logback" % "logback-classic" % "1.1.7",
      "org.postgresql" % "postgresql" % "9.4.1211",
      "com.h2database" % "h2" % "1.4.192",
      "com.lihaoyi" %% "pprint" % "0.4.2",
      "com.github.dnvriend" %% "akka-stream-extensions" % "0.0.2",
      "com.github.dnvriend" %% "akka-persistence-jdbc" % "2.6.7",
      "com.typesafe.slick" %% "slick" % slickVersion,
      "com.typesafe.slick" %% "slick-extensions" % "3.1.0",
      "com.typesafe.slick" %% "slick-hikaricp" % slickVersion exclude("com.zaxxer", "HikariCP-java6"),
      "com.zaxxer" % "HikariCP" % hikariCPVersion,
      "net.manub" %% "scalatest-embedded-kafka" % "0.8.0" % Test,
      "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
      "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % Test,
      "org.scalatest" %% "scalatest" % "3.0.0" % Test
    )
  }