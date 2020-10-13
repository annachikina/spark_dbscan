package org.apache.spark.mllib.clustering.util.commandLine

//private [dbscan]
trait NumberOfPointsInPartitionParsing [C <: CommonArgs with NumberOfPointsInPartitionArg] extends CommonArgsParser[C] {
  opt[Long] ("npp")
    .foreach { args.numberOfPoints = _ }

}


