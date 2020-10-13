package org.apache.spark.mllib.clustering.util.commandLine

import org.apache.spark.mllib.clustering.DbscanSettings
import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.apache.spark.mllib.clustering.DbscanSettings
import org.apache.spark.mllib.clustering.DbscanSettings

//private [dbscan]
class CommonArgs (
  var masterUrl: String = null,
  var jar: String = null,
  var inputPath: String = null,
  var outputPath: String = null,
  var distanceMeasure: DistanceMeasure = DbscanSettings.getDefaultDistanceMeasure,
  var debugOutputPath: Option[String] = None)
