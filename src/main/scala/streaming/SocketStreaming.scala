package streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object SocketStreaming {

  def main(args : Array[String]): Unit = {

    println(sys.env.get("HADOOP_HOME"))
    println(sys.env.get("LD_LIBRARY_PATH"))


    val conf = new SparkConf()
    conf.setAppName("SparkStreaming")
//    conf.setMaster("local[*]")
    conf.setMaster("spark://localhost:30052")
    conf.set("spark.submit.deployMode", "cluster")
    conf.setJars(Seq("/home/alex/IdeaProjects/spark-scala/target/spark-scala-1.0-SNAPSHOT.jar"))
    val context = new SparkContext(conf)

    val streamingContext = new StreamingContext(context, Seconds(10))
//    val rawDStream = streamingContext.socketTextStream("localhost", 17000)
    val rawDStream = streamingContext.textFileStream("file:///tmp/stream")
    rawDStream
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(item => item.swap)
      .transform(rdd => rdd.sortByKey(ascending = false))
      .foreachRDD(rdd => {
        rdd.take(10).foreach(x => Files.write(Paths.get("/tmp/file.txt"), x.toString().getBytes(StandardCharsets.UTF_8)))
      })


    streamingContext.start()
    streamingContext.awaitTermination()

    // cat TEXT_FILE | nc -lk 17000
  }
}
