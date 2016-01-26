import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by kufu on 16-1-18.
  */
class ReadFromSocket {

  def run(): Unit ={

    val conf = new SparkConf().setAppName("My Net Stream Testing")
      .setMaster("local[2]")
      .setSparkHome("/home/kufu/spark/spark-1.5.2-bin-hadoop2.6")
      .setJars(List("target/scala-2.10/my-net-stream-testing_2.10-1.0.jar"))

    val ssc = new StreamingContext(conf, Seconds(2))
    val streamData = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    val wordCounts = streamData.flatMap(_.split(" ")).map(word=>(word, 1)).reduceByKey(_+_)

    wordCounts.foreachRDD((rdd)=>{
      println("---------------------")
      println("get "+rdd.count()+" results:")
      rdd.foreach(pair=>println("\t("+pair._1+", "+pair._2+")"))
      println("---------------------")
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
