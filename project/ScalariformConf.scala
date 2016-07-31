package sbt

import com.typesafe.sbt.SbtScalariform
import sbt._

import scalariform.formatter.preferences.{AlignSingleLineCaseStatements, DoubleIndentClassDeclaration}

/**
  * `:=` assigns an initialization expression to a key
  * `+=` appends an initialization expression to the existing sequence in a key
  * `++=` appends an initialization of a sequence of values to the existing sequence in a key
  */

object ScalariformConf {
  lazy val settings: Seq[Setting[_]] = Seq(
    SbtScalariform.autoImport.scalariformPreferences := SbtScalariform.autoImport.scalariformPreferences.value
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(AlignSingleLineCaseStatements.MaxArrowIndent, 100)
      .setPreference(DoubleIndentClassDeclaration, true)
  )
}