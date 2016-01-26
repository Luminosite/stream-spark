import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util

/**
  * Created by kufu on 16-1-20.
  */
class SparkStreamingKafka {

  //TODO zero-data loss
  def kafkaStream(): Unit ={
    val conf = new SparkConf().setAppName("My Net Stream Testing")
      .setMaster("local[2]")
      .setSparkHome("/home/kufu/spark/spark-1.5.2-bin-hadoop2.6")
      .setJars(List("target/scala-2.10/my-net-stream-testing_2.10-1.0.jar"))

    val ssc = new StreamingContext(conf, Seconds(2))
    val topics = Map[String, Int]("myTopic" -> 5)//new util.HashMap[String, Integer]()
//    topics.put("myTopic", 3)
    val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "TestConsumerGroupID", topics, StorageLevel.MEMORY_ONLY)
    kafkaStream.foreachRDD(rdd=>{
      println("--------- a rdd ---------")
      rdd.foreach(data=>{
        println("pair:("+data._1+", "+data._2+")")
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
