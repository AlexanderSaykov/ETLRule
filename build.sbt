name := "ETLRule"

version := "0.1"

scalaVersion := "2.12.14"


libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "com.concurrentthought.cla" %% "command-line-arguments" % "0.6.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1"