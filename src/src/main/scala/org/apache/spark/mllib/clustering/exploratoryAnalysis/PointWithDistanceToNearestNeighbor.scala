package org.apache.spark.mllib.clustering.exploratoryAnalysis

import org.apache.spark.mllib.clustering.spatial.Point
import org.apache.spark.mllib.clustering.spatial.Point
import org.apache.spark.mllib.clustering.spatial.Point

//private [dbscan]
class PointWithDistanceToNearestNeighbor (pt: Point, d: Double = Double.MaxValue) extends  Point (pt) {
  var distanceToNearestNeighbor = d
}
