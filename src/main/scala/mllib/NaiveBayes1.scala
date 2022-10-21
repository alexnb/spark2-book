package mllib

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.{SparkConf, SparkContext}

object NaiveBayes1 {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkStreaming")
    conf.setMaster("local[*]")
    val context = new SparkContext(conf)

    var csvData = context.textFile("/home/alex/data/DigitalBreathTestData2014-DOUBLE.txt")
    val arrayData = csvData.map {
      csvLine =>
        val colData = csvLine.split(',')
        LabeledPoint(colData(0).toDouble,
          Vectors.dense(colData
            .drop(1)
          .map(_.toDouble)))
    }
    val divData = arrayData.randomSplit(Array(0.7, 0.3), seed = 13L)
    val trainDataSet = divData(0)
    val testDataSet = divData(1)
    val nbTrained = NaiveBayes.train(trainDataSet)
    val nbPredict = nbTrained.predict(testDataSet.map(_.features))

    val predictionAndLabel = nbPredict.zip(testDataSet.map(_.label))
    val accuracy = 100.0 * predictionAndLabel.filter(x => x._1 == x._2).count() /
      testDataSet.count()
    println(s"Accuracy: $accuracy")



  }

}
