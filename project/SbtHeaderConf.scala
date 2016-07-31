package sbt

import sbt.Keys._
import sbt._

import de.heikoseeberger.sbtheader.license.Apache2_0
import de.heikoseeberger.sbtheader.HeaderKey._
/**
  * `:=` assigns an initialization expression to a key
  * `+=` appends an initialization expression to the existing sequence in a key
  * `++=` appends an initialization of a sequence of values to the existing sequence in a key
  */

object SbtHeaderConf {
	lazy val settings: Seq[Setting[_]] = Seq(
		headers := Map(
		  "scala" -> Apache2_0("2016", "Dennis Vriend"),
		  "conf" -> Apache2_0("2016", "Dennis Vriend", "#")
		)
	)
}