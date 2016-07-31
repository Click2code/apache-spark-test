name := "apache-spark-test"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val akkaVersion = "2.4.8"
  Seq(
  	"org.apache.spark" %% "spark-core" % "2.0.0",
    "org.apache.spark" %% "spark-sql" % "2.0.0",
    "org.apache.spark" %% "spark-tags" % "2.0.0",
    "org.apache.spark" %% "spark-streaming" % "2.0.0",
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,
    "com.github.dnvriend" %% "akka-stream-extensions" % "0.0.1",
    "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
    "ch.qos.logback" % "logback-classic" % "1.1.7",
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
    "org.scalatest" %% "scalatest" % "2.2.6" % Test
  )
}

fork in Test := true

parallelExecution in Test := false

licenses +=("Apache-2.0", url("http://opensource.org/licenses/apache2.0.php"))

lazy val helloworld = (project in file("helloworld"))
	.settings((GenericConf.settings ++ SbtHeaderConf.settings ++ ScalariformConf.settings):_*)
   .enablePlugins(AutomateHeaderPlugin)
