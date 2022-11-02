package sparkml

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkMLIntroduction {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("SparkStreaming")
    conf.setMaster("local[*]")
    val context = new SparkContext(conf)
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._

    val df = session.range(10).toDF()
      .map(row => {
        val fileIndex = row.getAs[Long](0)
        fileIndex.toString
      }).toDF("label")

      val stringIndexer: StringIndexer = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("index")
        .setHandleInvalid("keep")

    val indexed = stringIndexer.fit(df).transform(df)
    indexed.show()

    val encoder = new OneHotEncoder()
      .setInputCol("index")
      .setOutputCol("vector")

    val encoded = encoder.fit(indexed).transform(indexed)
    encoded.show()

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("vector", "index"))
      .setOutputCol("features")

    val assembled = vectorAssembler.transform(encoded)
    assembled.show()

    val transformers = (stringIndexer :: encoder :: vectorAssembler :: Nil).toArray
    val pipeline = new Pipeline().setStages(transformers).fit(df)
    val transformed = pipeline.transform(df)
    transformed.show()

    val randomForest = new RandomForestClassifier()
      .setLabelCol("index")
      .setFeaturesCol("features")

    val model = new Pipeline().setStages(transformers :+ randomForest).fit(df)

    val randomForestResult = model.transform(df)
    randomForestResult.show()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("index")
//    val paramMap = ParamMap(evaluator.metricName -> "areaUnderROC")
    val evaluation = evaluator.evaluate(randomForestResult)
    println(evaluation)

    val paramGrid = new ParamGridBuilder()
      .addGrid(randomForest.numTrees, 3 :: 5 :: 10 :: Nil)
      .build()

    val crossValidator = new CrossValidator()
      .setEstimator(new Pipeline().setStages(transformers :+ randomForest))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)
      .setEvaluator(evaluator)
    val crossValidationModel = crossValidator.fit(df)
    val newPredictions = crossValidationModel.transform(df)
    println(evaluator.evaluate(newPredictions))

  }
}