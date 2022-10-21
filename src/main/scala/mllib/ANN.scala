package mllib

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

object ANN {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkStreaming")
    conf.setMaster("local[*]")
    val context = new SparkContext(conf)
    val session = SparkSession.builder().getOrCreate()

    //    val data = session.read.format("libsvm")
    //      .load("src/main/resources/ann/sample_multiclass_classification_data.txt")
    //    // Split the data into train and test
    //    val splits = data.randomSplit(Array(0.6, 0.4), seed = 1234L)

    val train = toDf(context, session, "src/main/resources/ann/train/")
    val test = toDf(context, session, "src/main/resources/ann/test/")

    // specify layers for the neural network:
    // input layer must be of size == features and output of size == classes
    val layers = Array[Int](400, 5, 5, 6)

    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)

    // train the model
    val model = trainer.fit(train)

    // compute accuracy on the test set
    val result = model.transform(test)
    result.show()
    val predictionAndLabels = result.select("prediction", "label")
    predictionAndLabels.show()
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")
  }

  private def toDf(context: SparkContext, session: SparkSession, dir: String): DataFrame = {
    import session.implicits._
    val files = new File(dir).listFiles()
      .sortBy(_.getName)
      .map(fileToDoubleArray)
      .map(org.apache.spark.ml.linalg.Vectors.dense)
      .toList

    session.range(files.length).toDF()
      .map(row => {
        val fileIndex = row.getAs[Long](0)
        (fileIndex.toInt, files(row.getAs[Long](0).toInt))
      }).toDF("label", "features")
  }

  private def fileToDoubleArray(f: File) = {
    val source = scala.io.Source.fromFile(f)
    val lines = try source.mkString finally source.close()
    lines.split(' ').map(_.toDouble)
  }
}
