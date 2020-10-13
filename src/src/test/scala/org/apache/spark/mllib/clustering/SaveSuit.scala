package org.apache.spark.mllib.clustering

class SaveSuit extends DbscanSuiteBase {

  val clusteredPoints = sc.parallelize (Array ( create2DPoint(0.0, 0.0).withClusterId(1),
    create2DPoint(1.0, 0.0).withClusterId(1),
    create2DPoint(0.0, 1.0).withClusterId(1),
    create2DPoint(1.0, 1.0).withClusterId(1),

    create2DPoint(3.0, 0.0).withClusterId(2),
    create2DPoint(4.0, 0.0).withClusterId(2),
    create2DPoint(3.0, 1.0).withClusterId(2),
    create2DPoint(4.0, 1.0).withClusterId(2),

    create2DPoint(1.0, 3.0).withClusterId(DbscanModel.NoisePoint),
    create2DPoint(3.0, 3.0).withClusterId(DbscanModel.NoisePoint)
  ))


  val settings3 = new DbscanSettings ().withEpsilon(1.5).withNumberOfPoints(3)
  val model3 = new DbscanModel (clusteredPoints, settings3)

  val settings4 = new DbscanSettings ().withEpsilon(1.5).withNumberOfPoints(4).withTreatBorderPointsAsNoise(true)
  val model4 = new DbscanModel (clusteredPoints, settings4)

  test("Test 1. Command: |fit than save") {
    model3.save(sc, "testmodel")
    val savedModel = DbscanModel.load(sc, "testmodel")

    val predictedClusterId = savedModel.predict(create2DPoint (0.5, 0.5))

    predictedClusterId should equal (1L)

  }

}
