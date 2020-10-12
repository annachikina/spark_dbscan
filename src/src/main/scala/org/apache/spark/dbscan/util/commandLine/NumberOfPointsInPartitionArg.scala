package org.apache.spark.dbscan.util.commandLine

import org.apache.spark.dbscan.spatial.rdd.PartitioningSettings


private [dbscan] trait NumberOfPointsInPartitionArg {
  var numberOfPoints: Long = PartitioningSettings.DefaultNumberOfPointsInBox
}
