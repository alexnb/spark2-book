package streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SocketStreaming {

  def main(args : Array[String]): Unit = {

    println(sys.env.get("HADOOP_HOME"))
    println(sys.env.get("LD_LIBRARY_PATH"))


    val conf = new SparkConf()
    conf.setAppName("SparkStreaming")
    conf.setMaster("local[*]")
    val context = new SparkContext(conf)

    val streamingContext = new StreamingContext(context, Seconds(10))
    val rawDStream = streamingContext.socketTextStream("localhost", 17000)
//    val rawDStream = streamingContext.textFileStream("hdfs://f-vm1.ser.net:9000/test/mywords.txt")
    rawDStream
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(item => item.swap)
      .transform(rdd => rdd.sortByKey(ascending = false))
      .foreachRDD(rdd => {
        rdd.take(10).foreach(x => println("List: " + x))
      })
    streamingContext.start()
    streamingContext.awaitTermination()

    // cat TEXT_FILE | nc -lk 17000
  }
}
