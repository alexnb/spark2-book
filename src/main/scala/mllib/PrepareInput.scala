package mllib

import org.apache.spark.{SparkConf, SparkContext}

object PrepareInput {

  def enumerateCsvRecord(colData: Array[String]): String = {
    val colVal1 = colData(7) match {
      case "Male" => 0
      case "Female" => 1
      case "Unknown" => 2
      case _ => 99
    }
    val colVal2 = colData(0) match {
      case "Moving Traffic Violation" => 0
      case "Other" => 1
      case "Road Traffic Collision" => 2
      case "Suspicion of Alcohol" => 3
      case _ => 99
    }
    val colVal3 = colData(3) match {
      case "Weekday" => 0
      case "Weekend" => 1
      case _ => 99
    }
    val colVal4 = colData(4) match {
      case "12am-4am" => 0
      case "4am-8am" => 1
      case "8am-12pm" => 2
      case "12pm-4pm" => 3
      case "4pm-8pm" => 4
      case "8pm-12pm" => 5
      case _ => 99
    }
    val colVal5 = colData(5)
    val colVal6 = colData(6) match {
      case "16-19" => 0
      case "20-24" => 1
      case "25-29" => 2
      case "30-39" => 3
      case "40-49" => 4
      case "50-59" => 5
      case "60-69" => 6
      case "70-98" => 7
      case "Other" => 8
      case _ => 99
    }
    s"$colVal1,$colVal2,$colVal3,$colVal4,$colVal5,$colVal6"
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkStreaming")
    conf.setMaster("local[*]")
    val context = new SparkContext(conf)

    var csvData = context.textFile("/home/alex/data/DigitalBreathTestData2014.txt")
    println(s"Records in ${csvData.count()}")
    val enumRddData = csvData.map {
      csvLine =>
        val colData = csvLine.split(',')
        enumerateCsvRecord(colData)
    }
    println(s"Records out ${csvData.count()}")
    enumRddData.saveAsTextFile("/home/alex/data/DigitalBreathTestData2014-out")
  }

}
