package org.apache.spark.dbscan.util.commandLine

import org.apache.spark.dbscan.DbscanSettings

private [dbscan] trait EpsArg {
  var eps: Double = DbscanSettings.getDefaultEpsilon
}
