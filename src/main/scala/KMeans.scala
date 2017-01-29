import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.clustering.KMeans
import breeze.linalg.{DenseVector, squaredDistance}


/**
  * Assumptions:
  * Iterative solution
  * Randomly pick k centers from dataset initially
  * Using 2-D sample dataset for simplicity, but still support N-D dataset
  * Broadcast centers to workers
  * Created by lzhang on 1/28/17.
  */
case object KMeans {

  /**
    * method to find the closest centers
    *
    * @param p       target point
    * @param centers an array of center points
    * @return center's corrsponding index
    */
  def closestPoint(p: DenseVector[Double], centers: Array[DenseVector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }

  /**
    * My own KMeans implementation with breeze's library
    *
    * @param data         a RDD of DenseVector[Double]
    * @param k            number of cluster expected
    * @param maxIteration maximun number of interation can be run
    */
  def run(data: RDD[DenseVector[Double]], k: Int, maxIteration: Int, sc: SparkContext): RDD[(Int, DenseVector[Double])] = {
    // randomly pick k centers
    val centers: Array[DenseVector[Double]] = data
      .takeSample(withReplacement = false, k, 42) // Array[Vector[Double]]

    val cachedData = data.cache()
    var result: RDD[(Int, DenseVector[Double])] = sc.emptyRDD[(Int, DenseVector[Double])]
    var iteration = maxIteration
    while (iteration > 0) {
      val kPoints = sc.broadcast(centers)
      val closest = cachedData.map(p => (closestPoint(p, kPoints.value), (p, 1)))
      val pointStats = closest.reduceByKey { case ((p1, c1), (p2, c2)) => (p1 + p2, c1 + c2) }

      // new centers
      val newPoints = pointStats.map { pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))
      }.collectAsMap()

      //      val newCenters = new Array[DenseVector[Double]](k)
      for (newP <- newPoints) {
        centers(newP._1) = newP._2
      }
      kPoints.unpersist(blocking = true)
      iteration = iteration - 1
      if (iteration == 0) {
        result = closest.map(p => (p._1, p._2._1)).cache()
      }
    }

    println("My solution's Final Centers: ")
    centers.foreach(println)
    result.foreach(println)
    result
  }

  /**
    * large test dataset
    *
    * @return a sequence of DenseVector[Double]
    */
  def largeDataSet(): Seq[DenseVector[Double]] = {
    // use 2-D points for simplicity
    val large = Seq(DenseVector(10.8348626966492, 18.7800980127523),
      DenseVector(10.259545802461, 23.4515683763173),
      DenseVector(11.7396623802245, 17.7026240456956),
      DenseVector(12.4277617893575, 19.4887691804508),
      DenseVector(10.1057940183815, 18.7332929859685),
      DenseVector(11.0118378554584, 20.9773232834654),
      DenseVector(7.03171204763376, 19.1985058633283),
      DenseVector(6.56491029696013, 21.5098251711267),
      DenseVector(10.7751248849735, 22.1517666115673),
      DenseVector(8.90149362263775, 19.6314465074203),
      DenseVector(11.931275122466, 18.0462702532436),
      DenseVector(11.7265904596619, 16.9636039793709),
      DenseVector(11.7493214262468, 17.8517235677469),
      DenseVector(12.4353462881232, 19.6310467981989),
      DenseVector(13.0838514816799, 20.3398794353494),
      DenseVector(7.7875624720831, 20.1569764307574),
      DenseVector(11.9096128931784, 21.1855674228972),
      DenseVector(8.87507602702847, 21.4823134390704),
      DenseVector(7.91362116378194, 21.325928219919),
      DenseVector(26.4748241341303, 9.25128245838802),
      DenseVector(26.2100410238973, 5.06220487544192),
      DenseVector(28.1587146197594, 3.70625885635717),
      DenseVector(26.8942422516129, 5.02646862012427),
      DenseVector(23.7770902983858, 7.19445492687232),
      DenseVector(23.6587920739353, 3.35476798095758),
      DenseVector(23.7722765903534, 3.74873642284525),
      DenseVector(23.9177161897547, 8.1377950229489),
      DenseVector(22.4668345067162, 8.9705504626857),
      DenseVector(24.5349708443852, 5.00561881333415),
      DenseVector(24.3793349065557, 4.59761596097384),
      DenseVector(27.0339042858296, 4.4151109960116),
      DenseVector(21.8031183153743, 5.69297814349064),
      DenseVector(22.636600400773, 2.46561420928429),
      DenseVector(25.1439536911272, 3.58469981317611),
      DenseVector(21.4930923464916, 3.28999356823389),
      DenseVector(23.5359486724204, 4.07290025106778),
      DenseVector(22.5447925324242, 2.99485404382734),
      DenseVector(25.4645673159779, 7.54703465191098))
    large
  }

  /**
    * Using my kmeans implementation
    *
    * @param sc
    */
  def mySolution(sc: SparkContext) = {
    val points = Seq(
      DenseVector(0.0, 0.0),
      DenseVector(0.0, 0.1),
      DenseVector(0.1, 0.0),
      DenseVector(9.0, 0.0),
      DenseVector(9.0, 0.2),
      DenseVector(9.2, 0.0)
    )
    // test larget dataset
    //    val points = largeDataSet()
    val data = sc.parallelize(points, 3)
    val k = 2
    val maxIteration = 2

    run(data, k, maxIteration, sc)
    sc.stop()
  }

  /**
    * Using spark's mllib's kmeans
    *
    * @param sqlContext
    */
  def sparkSolution(sqlContext: SQLContext): Unit = {
    // mllib's KMeans
    // Crates a DataFrame
    val dataset: DataFrame = sqlContext.createDataFrame(Seq(
      (1, Vectors.dense(0.0, 0.0)),
      (2, Vectors.dense(0.0, 0.1)),
      (3, Vectors.dense(0.1, 0.0)),
      (4, Vectors.dense(9.0, 0.0)),
      (5, Vectors.dense(9.0, 0.2)),
      (6, Vectors.dense(9.2, 0.0))
    )).toDF("id", "features")

    // Trains a k-means model
    val kmeans = new KMeans()
      .setK(2)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
    val model = kmeans.fit(dataset)

    // Shows the result
    println("Spark solution's Final Centers: ")
    model.transform(dataset).foreach(println)
    model.clusterCenters.foreach(println)
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KMeans for Yewno").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // compare spark's solution with my solution with same dataset
    sparkSolution(sqlContext)
    mySolution(sc)
  }
}
