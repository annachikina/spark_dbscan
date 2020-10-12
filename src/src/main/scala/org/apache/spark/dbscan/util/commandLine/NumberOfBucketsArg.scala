package org.apache.spark.dbscan.util.commandLine

import org.apache.spark.dbscan.exploratoryAnalysis.ExploratoryAnalysisHelper

private [dbscan] trait NumberOfBucketsArg {
  var numberOfBuckets: Int = ExploratoryAnalysisHelper.DefaultNumberOfBucketsInHistogram
}
