package mllib

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object KMeans1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkStreaming")
    conf.setMaster("local[*]")
    val context = new SparkContext(conf)

    var csvData = context.textFile("/home/alex/data/DigitalBreathTestData2014-DOUBLE.txt")
    val vectorData = csvData.map(csvLine => Vectors.dense(csvLine.split(',').map(_.toDouble)))

    val kMeans = new KMeans
    kMeans.setK(3)
    kMeans.setMaxIterations(50)
    kMeans.setInitializationMode(KMeans.K_MEANS_PARALLEL)
    kMeans.setEpsilon(1e-4)
    vectorData.cache
    val kMeansModel = kMeans.run(vectorData)
    val kMeansCost = kMeansModel.computeCost(vectorData)
    println("Input data rows : " + vectorData.count())
    println("K Means Cost    : " + kMeansCost)
    kMeansModel.clusterCenters.foreach(println)
    val clusterRddInt = kMeansModel.predict(vectorData)
    clusterRddInt.countByValue.toList.foreach(println)
  }
}
