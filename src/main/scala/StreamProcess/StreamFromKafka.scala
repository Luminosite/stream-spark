package StreamProcess

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Scan, ResultScanner, Put, Increment}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by kufu on 16-1-25.
  */
class StreamFromKafka {

  def run(): Unit ={

    val conf = new SparkConf().setAppName("My Net Stream Testing")
      .setMaster("local[2]")
      .setSparkHome("/home/kufu/spark/spark-1.5.2-bin-hadoop2.6")
      .setJars(List("target/scala-2.10/my-net-stream-testing_2.10-1.0.jar"))

    val ssc = new StreamingContext(conf, Seconds(2))
    val topics = Map[String, Int]("myTopic" -> 5)
    val kafkaStream = KafkaUtils.createStream(
      ssc,
      "localhost:2181",
      "TestConsumerGroupID",
      topics,
      StorageLevel.MEMORY_ONLY)

    val wordCount = kafkaStream.flatMap(pair=>{pair._2.split(" ")}).map(word=>(word, 1)).reduceByKey(_+_)

    //HBase configuration
    val tableName = "MyTestTable"
    val familyName = "f1"
    val qualifierName = "c1"
    val hbaseConfig = HBaseConfiguration.create()
    val jobConfig = new JobConf(hbaseConfig, this.getClass)
    jobConfig.setOutputFormat(classOf[TableIncrementFormat])
    jobConfig.set(TableIncrementFormat.OUTPUT_TABLE, tableName)

    wordCount.foreachRDD(rdd=>{
      println("--------- a rdd ---------")
      rdd.foreach(data=>{
        println("pair:("+data._1+", "+data._2+")")
      })

      rdd.map(Converter.toIncrement(familyName, qualifierName)).saveAsHadoopDataset(jobConfig)

      if(!rdd.isEmpty()){
        //read data from HBaseTable
        val hbaseConnection = new HBaseConnection(tableName, List(familyName))
        hbaseConnection.openOrCreateTable()

        val scan = new Scan()
        scan.addFamily(Bytes.toBytes(familyName))
        //get encapsulated data from result scanner
        val result:ResultScanner = hbaseConnection.scan(scan)
        val resultList = HTableData.getHTableData(result)

        hbaseConnection.close()

        //prepare message to publish
        val message = new StringBuilder
        message++="statistics message:\n"
        resultList.foreach(data=>{
          message++=Bytes.toString(data.row)
          message++=":"
          message++=data.getValueString
          message++="\n"
        })
        println(message)

        //publish to statistics topic in kafka
        val kafkaConnection = new KafkaConnection(
          List("localhost:9095", "localhost:9096", "localhost:9097"),
          "statistics")
        kafkaConnection.send(message.toString)
        kafkaConnection.close()
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}

object Converter{
  def convertToPut(tuple:(String, Int)):(ImmutableBytesWritable, Put) = {
    val rowKey = Bytes.toBytes(tuple._1)
    val increment = new Put(rowKey)
    val family = Bytes.toBytes("f1")
    val qualifier = Bytes.toBytes("c1")
    val num:Long = tuple._2
    increment.add(family, qualifier, Bytes.toBytes(num))

    (new ImmutableBytesWritable(rowKey), increment)
  }

  def toIncrement(familyName:String, qualifierName:String)
          : ((String, Int)) => (ImmutableBytesWritable, Increment) =  {
    def ret(tuple:(String, Int)):(ImmutableBytesWritable, Increment) = {
      val rowKey = Bytes.toBytes(tuple._1)
      val increment = new Increment(rowKey)
      val family = Bytes.toBytes(familyName)
      val qualifier = Bytes.toBytes(qualifierName)
      val num:Long = tuple._2
      increment.addColumn(family, qualifier, num)

      (new ImmutableBytesWritable(rowKey), increment)
    }
    ret
  }
}
