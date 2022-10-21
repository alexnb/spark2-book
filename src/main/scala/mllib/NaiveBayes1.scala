package mllib

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.{SparkConf, SparkContext}

object NaiveBayes1 {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkStreaming")
    conf.setMaster("local[*]")
    val context = new SparkContext(conf)

    var csvData = context.textFile("/home/alex/data/DigitalBreathTestData2014-DOUBLE.txt")
//    var csvData = context.textFile("/home/alex/data/simpledata1.txt")
//    val arrayData = csvData.map(csvLine => {
//      val colData = csvLine.split(',')
//      LabeledPoint(colData(0).toDouble,
//        Vectors.sparse(5, Array(0, 1, 2, 3, 4), colData
//          .drop(1)
//          .map(_.toDouble)))
//    })
    var arrayData = MLUtils.loadLibSVMFile(context, "/home/alex/data/simpledata_svm.txt")
    arrayData = arrayData.map(r => {
      LabeledPoint(r.label, Vectors.dense(r.features.toDense.values.takeRight(150)))
    })
//    arrayData1.take(10).foreach(println)
    arrayData.take(10).foreach(println)

    val divData = arrayData.randomSplit(Array(0.7, 0.3), seed = 13L)
    val trainDataSet = divData(0)
    val testDataSet = divData(1)
    val nbTrained = NaiveBayes.train(trainDataSet)
    val nbPredict = nbTrained.predict(testDataSet.map(_.features))

    testDataSet.map(_.features).take(100).foreach(println)

    val predictionAndLabel = nbPredict.zip(testDataSet.map(_.label))
    val accuracy = 100.0 * predictionAndLabel.filter(x => x._1 == x._2).count() /
      testDataSet.count()
    println(s"Accuracy: $accuracy")



  }

}
