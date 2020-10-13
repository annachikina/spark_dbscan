package org.apache.spark.mllib.clustering

import org.apache.spark.mllib.clustering.spatial.{DistanceAnalyzer, Point}
import org.apache.commons.math3.ml.distance.{CanberraDistance, ChebyshevDistance, DistanceMeasure, EarthMoversDistance, EuclideanDistance, ManhattanDistance}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.pmml.PMMLExportable
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/** Represents results calculated by DBSCAN algorithm.
  *
  * This object is returned from [[org.apache.spark.mllib.clustering.Dbscan.run]] method.
  * You cannot instantiate it directly
  */
class DbscanModel(val allPoints: RDD[Point],
                  val settings: DbscanSettings)
  extends Saveable with Serializable with PMMLExportable {


  def eps: Double = settings.epsilon

  def distanceMeasure: String = settings.distanceMeasure.toString

  def minPts: Int = settings.numberOfPoints

  def treatBorderPointsAsNoise: Boolean = settings.treatBorderPointsAsNoise

  val points: RDD[Point] = allPoints


  /** Predicts which cluster a point would belong to
    *
    * @param newPoint A [org.apache.spark.mllib.clustering.PointCoordinates] for which you want to make a prediction
    * @return If the point can be assigned to a cluster, then this cluster's ID is returned.
    *         If the point appears to be a noise point, then
    *         [org.apache.spark.mllib.clustering.DbscanModel.NoisePoint] is returned.
    *         If the point is surrounded by so many noise points that they can constitute a new
    *         cluster, then [org.apache.spark.mllib.clustering.DbscanModel.NewCluster] is returned
    */
  def findId (newPoint: Point): ClusterId = {
    val distanceAnalyzer = new DistanceAnalyzer(settings)
    val neighborCountsByCluster = distanceAnalyzer.findNeighborsOfNewPoint(allPoints, newPoint.coordinates)
      .map ( x => (x.clusterId, x) )
      .countByKey()

    val neighborCountsWithoutNoise = neighborCountsByCluster.filter(_._1 != DbscanModel.NoisePoint)
    val possibleClusters = neighborCountsWithoutNoise.filter(_._2 >= settings.numberOfPoints-1)
    val noisePointsCount = if (neighborCountsByCluster.contains(DbscanModel.NoisePoint)) {
      neighborCountsByCluster (DbscanModel.NoisePoint)
    }
    else {
      0L
    }

    if (possibleClusters.size >= 1) {

      // If a point is surrounded by >= numPts points which belong to one cluster, then the point should be assigned to that cluster
      // If there are more than one clusters, then the cluster will be chosen arbitrarily

      possibleClusters.keySet.head
    }
    else if (neighborCountsWithoutNoise.size >= 1 && !settings.treatBorderPointsAsNoise) {

      // If there is not enough surrounding points, then the new point is a border point of a cluster
      // In this case, the prediction depends on treatBorderPointsAsNoise flag.
      // If it allows assigning border points to clusters, then the new point will be assigned to the cluster
      // If there are many clusters, then one of them will be chosen arbitrarily

      neighborCountsWithoutNoise.keySet.head
    }
    else if (noisePointsCount >= settings.numberOfPoints-1) {

      // The point is surrounded by sufficiently many noise points so that together they will constitute a new cluster

      DbscanModel.NewCluster
    }
    else {

      // If none of the above conditions are met, then the new point is noise

      DbscanModel.NoisePoint
    }
  }

  def predict (newPoint: Point): ClusterId = findId(newPoint)

  def predict (newPoints: RDD[Point]): RDD[ClusterId] = {
    newPoints.map(p => findId(p))
  }


  /** Returns only noise points
    *
    * @return
    */
  def noisePoints: RDD[Point] = { allPoints.filter(_.clusterId == DbscanModel.NoisePoint) }

  /** Returns points which were assigned to clusters
    *
    * @return
    */
  def clusteredPoints: RDD[Point] = { allPoints.filter( _.clusterId != DbscanModel.NoisePoint) }

  override def save(sc: SparkContext, path: String): Unit = {
    DbscanModel.SaveLoadV2_0.save(sc, this, path)
  }

  override protected def formatVersion: String = "2.0"
}

/** Contains constants which designate cluster ID
  *
  */
object DbscanModel extends Loader[DbscanModel] {

  /** Designates noise points
    *
    */
  val NoisePoint: ClusterId = 0

  /** Indicates that a new cluster would appear in a [[org.apache.spark.mllib.clustering.DbscanModel]] if
    * a new point was added to it
    */
  val NewCluster: ClusterId = -1

  /** Initial value for cluster ID of each point.
    *
    */
//  private[dbscan]
  val UndefinedCluster: ClusterId = -2

  override def load(sc: SparkContext, path: String): DbscanModel = {
    val (loadedClassName, version, metadata) = org.apache.spark.mllib.util.Loader.loadMetadata(sc, path)
//    val classNameV1_0 = SaveLoadV1_0.thisClassName
    val classNameV2_0 = SaveLoadV2_0.thisClassName
    (loadedClassName, version) match {
//      case (className, "1.0") if className == classNameV1_0 =>
//        SaveLoadV1_0.load(sc, path)
      case (className, "2.0") if className == classNameV2_0 =>
        SaveLoadV2_0.load(sc, path)
      case _ => throw new Exception(
        s"DbscanModel.load did not recognize model with (className, format version):" +
          s"($loadedClassName, $version).  Supported:\n" +
//          s"  ($classNameV1_0, 1.0)\n" +
          s"  ($classNameV2_0, 2.0)")
    }
  }

  private[clustering] object SaveLoadV2_0 {

    private val thisFormatVersion = "2.0"

    private[clustering] val thisClassName = "org.apache.spark.mllib.clustering.DbscanModel"

    def save(sc: SparkContext, model: DbscanModel, path: String): Unit = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~ ("eps" -> model.eps)
          ~ ("minPts" -> model.minPts) ~ ("treatBorderPointsAsNoise" -> model.treatBorderPointsAsNoise)
          ~ ("distanceMeasure" -> model.distanceMeasure)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))
//      spark.createDataFrame(model.points).write.parquet(Loader.dataPath(path))
      model.points.coalesce(1).saveAsTextFile(Loader.metadataPath(path))
    }

    def load(sc: SparkContext, path: String): DbscanModel = {
      implicit val formats = DefaultFormats
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)
      val eps = (metadata \ "eps").extract[Double]
      val minPts = (metadata \ "minPts").extract[Int]
      val treatBorderPointsAsNoise = (metadata \ "treatBorderPointsAsNoise").extract[Boolean]
      val distanceMeasureString = (metadata \ "distanceMeasure").extract[String]
      val distanceMeasure = distanceMeasureString match {
        case """.*EuclideanDistance.*""" => new EuclideanDistance()
        case """.*CanberraDistance.*""" => new CanberraDistance()
        case """.*ChebyshevDistance.*""" => new ChebyshevDistance()
        case """EarthMoversDistance""" => new EarthMoversDistance()
        case """ManhattanDistance""" => new ManhattanDistance()
        case _ => new EuclideanDistance()
      }
//      val points = spark.read.parquet(Loader.dataPath(path))
//      val allPoints = points.rdd
      val stringRDD: RDD[String] = spark.sparkContext.textFile(Loader.dataPath(path))
      val arrayRDD = stringRDD.map(_.split(",").toSeq.toArray)
      val allPoints: RDD[Point] = arrayRDD.map(line => new Point(line.map(_.toDouble)))
      val settings = new DbscanSettings()
        .withEpsilon(eps)
        .withNumberOfPoints(minPts)
        .withDistanceMeasure(distanceMeasure)
        .withTreatBorderPointsAsNoise(treatBorderPointsAsNoise)
      new DbscanModel(allPoints, settings)
    }
  }

}
