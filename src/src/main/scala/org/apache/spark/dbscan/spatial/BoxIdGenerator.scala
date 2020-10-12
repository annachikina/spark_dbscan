package org.apache.spark.dbscan.spatial

import org.apache.spark.dbscan._

private [dbscan] class BoxIdGenerator (val initialId: BoxId) {
  var nextId = initialId

  def getNextId (): BoxId = {
    nextId += 1
    nextId
  }
}
