package org.apache.spark.dbscan.util.commandLine

import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.apache.spark.dbscan.DbscanSettings

private [dbscan] class CommonArgs (
  var masterUrl: String = null,
  var jar: String = null,
  var inputPath: String = null,
  var outputPath: String = null,
  var distanceMeasure: DistanceMeasure = DbscanSettings.getDefaultDistanceMeasure,
  var debugOutputPath: Option[String] = None)
