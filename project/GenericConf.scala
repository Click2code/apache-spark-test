package sbt

import sbt.Keys._
import sbt._

/**
  * `:=` assigns an initialization expression to a key
  * `+=` appends an initialization expression to the existing sequence in a key
  * `++=` appends an initialization of a sequence of values to the existing sequence in a key
  */

object GenericConf {
  lazy val settings: Seq[Setting[_]] = Seq(    
    parallelExecution in Test := false,
    fork in Test := true
  )
}