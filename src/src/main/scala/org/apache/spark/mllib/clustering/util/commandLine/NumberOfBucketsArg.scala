package org.apache.spark.mllib.clustering.util.commandLine

import org.apache.spark.mllib.clustering.exploratoryAnalysis.ExploratoryAnalysisHelper
import org.apache.spark.mllib.clustering.exploratoryAnalysis.ExploratoryAnalysisHelper
import org.apache.spark.mllib.clustering.exploratoryAnalysis.ExploratoryAnalysisHelper

//private [dbscan]
trait NumberOfBucketsArg {
  var numberOfBuckets: Int = ExploratoryAnalysisHelper.DefaultNumberOfBucketsInHistogram
}
