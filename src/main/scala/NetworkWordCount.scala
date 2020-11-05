package org.apache.spark.examples.streaming
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object NetworkWordCount {

  val shutdownMarker = "/user/hadoop/stop_sparkStreaming"
  // flag to stop the spark streaming service
  var stopFlag: Boolean = false

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]").set("spark.streaming.stopGracefullyOnShutdown", "true")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(1))

    /** 流处理过程 */
    var stringRDD: RDD[(String, Int)] = sc.makeRDD(List())
//    var allWordCountsList: List[(String, Int)] = List()
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_ONLY)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).groupByKey()
    wordCounts.foreachRDD(
      rdd => {
        System.out.println(stringRDD.count())
        stringRDD = stringRDD.++(rdd.map(word => (word._1, word._2.sum)))
      }

    )

    ssc.start()
    val checkIntervalMillis = 10000
    var isStopped: Boolean = false
    while (!stopFlag) {
      isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis)
      if (isStopped) {
        println("WARNING!!! The spark streaming context is stopped. Exiting application ......")
      } else {
        println("spark streaming is still running......")
      }

      toShutDown_SparkStreaming
      if (!isStopped && stopFlag) {
        println("======> to stop ssc right now")
        //第一个true：停止相关的SparkContext。无论这个流媒体上下文是否已经启动，底层的SparkContext都将被停止
        //第二个true：等待所有接收到的数据的处理完成，然后优雅地停止
        ssc.stop(false, true)
        println("<====== ssc is stopped !!!")
      }
    }

//    println(allWordCountsList.size)
    System.out.println("End")

//    System.out.println(stringRDD.count())
    stringRDD = stringRDD.reduceByKey(_+_)
    System.out.println(stringRDD.count())

    System.out.println("End1")
  }


  def toShutDown_SparkStreaming = {
    if (!stopFlag) {
      // 检查是否要停止spark streaming service
      val fs = FileSystem.get(new Configuration())
      // 如果shutdownMarker目录存在，则停止服务
      stopFlag = fs.exists(new Path(shutdownMarker))
    }
  }


}