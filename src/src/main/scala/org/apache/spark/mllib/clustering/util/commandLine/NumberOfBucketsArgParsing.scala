package org.apache.spark.mllib.clustering.util.commandLine

//private [dbscan]
trait NumberOfBucketsArgParsing [C <: CommonArgs with NumberOfBucketsArg]
  extends CommonArgsParser[C] {

  opt[Int] ("numBuckets")
    .foreach { args.numberOfBuckets = _ }
    .valueName("<numBuckets>")
    .text("Number of buckets in a histogram")
}
