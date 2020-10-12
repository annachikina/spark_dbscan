package org.apache.spark.dbscan.exploratoryAnalysis

import org.apache.spark.dbscan.spatial.Point

private [dbscan] class PointWithDistanceToNearestNeighbor (pt: Point, d: Double = Double.MaxValue) extends  Point (pt) {
  var distanceToNearestNeighbor = d
}
