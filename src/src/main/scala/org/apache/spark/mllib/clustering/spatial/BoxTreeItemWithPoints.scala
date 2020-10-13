package org.apache.spark.mllib.clustering.spatial

import org.apache.spark.mllib.clustering.util.collection.SynchronizedArrayBuffer
import org.apache.spark.mllib.clustering.util.collection.SynchronizedArrayBuffer
import org.apache.spark.mllib.clustering.util.collection.SynchronizedArrayBuffer

//private [dbscan]
class BoxTreeItemWithPoints (b: Box,
  val points: SynchronizedArrayBuffer[Point] = new SynchronizedArrayBuffer[Point] (),
  val adjacentBoxes: SynchronizedArrayBuffer[BoxTreeItemWithPoints] = new SynchronizedArrayBuffer[BoxTreeItemWithPoints] ())
  extends BoxTreeItemBase [BoxTreeItemWithPoints] (b) {
}
