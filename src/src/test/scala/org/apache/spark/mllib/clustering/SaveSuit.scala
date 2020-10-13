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


  val settings1 = new DbscanSettings ().withEpsilon(1.5).withNumberOfPoints(3)
  val model1 = new DbscanModel (clusteredPoints, settings1)

  val settings2 = new DbscanSettings ().withEpsilon(1.5).withNumberOfPoints(4).withTreatBorderPointsAsNoise(true)
  val model2 = new DbscanModel (clusteredPoints, settings2)

  test("Test 1. Command: |fit than save") {
    model1.save(sc, "model1")
    val savedModel = DbscanModel.load(sc, "model1")

    val predictedClusterId = savedModel.predict(create2DPoint (0.5, 0.5))

    predictedClusterId should equal (1L)

  }

}
