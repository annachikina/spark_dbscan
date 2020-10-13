package org.apache.spark.mllib.clustering.util.commandLine

import org.apache.spark.mllib.clustering.DbscanSettings
import org.apache.spark.mllib.clustering.DbscanSettings
import org.apache.spark.mllib.clustering.DbscanSettings

//private [dbscan]
trait EpsArg {
  var eps: Double = DbscanSettings.getDefaultEpsilon
}
