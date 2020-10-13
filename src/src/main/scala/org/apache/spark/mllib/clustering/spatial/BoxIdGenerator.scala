package org.apache.spark.mllib.clustering.spatial

import org.apache.spark.mllib.clustering._

//private [dbscan]
class BoxIdGenerator (val initialId: BoxId) {
  var nextId = initialId

  def getNextId (): BoxId = {
    nextId += 1
    nextId
  }
}
