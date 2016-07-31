name := "apache-spark-test"

organization := "com.github.dnvriend"

version := "1.0.0"

scalaVersion := "2.11.8"

fork in Test := true

parallelExecution in Test := false

licenses += ("Apache-2.0", url("http://opensource.org/licenses/apache2.0.php"))

lazy val jobs = (project in file("jobs"))
  .settings(GenericConf.settings ++ SbtHeaderConf.settings ++ ScalariformConf.settings ++ DependenciesConf.settings: _*)
  .enablePlugins(AutomateHeaderPlugin)

lazy val helloworld = (project in file("helloworld"))
  .settings(GenericConf.settings ++ SbtHeaderConf.settings ++ ScalariformConf.settings ++ DependenciesConf.settings: _*)
  .dependsOn(jobs)
  .enablePlugins(AutomateHeaderPlugin)

lazy val restpi = (project in file("restpi"))
  .settings(GenericConf.settings ++ SbtHeaderConf.settings ++ ScalariformConf.settings ++ DependenciesConf.settings: _*)
  .dependsOn(jobs)
  .enablePlugins(AutomateHeaderPlugin)
