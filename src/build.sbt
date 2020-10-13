name := "spark_dbscan"

organization := "org.apache"

version := "0.0.4"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.1"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.4" % Test

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.2"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

test in assembly := {}