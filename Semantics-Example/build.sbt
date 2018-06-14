name := "Semantics-Example"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"

autoCompilerPlugins := true
//addCompilerPlugin("scala.tools.semantics" %% "SBT-SemanticsAware" % "0.0.1-SNAPSHOT")
addCompilerPlugin("scala.tools.semantics" %% "scala-compiler-plugin" % "0.0.1-SNAPSHOT")
// https://mvnrepository.com/artifact/com.github.mlangc/bracket-expression-beautifier
libraryDependencies += "com.github.mlangc" %% "bracket-expression-beautifier" % "1.0"
// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test

// https://mvnrepository.com/artifact/com.github.mlangc/bracket-expression-beautifier
libraryDependencies += "com.github.mlangc" %% "bracket-expression-beautifier" % "1.0"

// https://mvnrepository.com/artifact/junit/junit
libraryDependencies += "com.novocode" % "junit-interface" % "0.10" % Test

