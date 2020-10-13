package org.apache.spark.mllib.clustering.util.commandLine

import org.apache.spark.mllib.clustering.spatial.rdd.PartitioningSettings
import org.apache.spark.mllib.clustering.spatial.rdd.PartitioningSettings
import org.apache.spark.mllib.clustering.spatial.rdd.PartitioningSettings


//private [dbscan]
trait NumberOfPointsInPartitionArg {
  var numberOfPoints: Long = PartitioningSettings.DefaultNumberOfPointsInBox
}
