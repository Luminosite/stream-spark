import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by kufu on 16-1-18.
  */
class HBaseReading {

  def run(): Unit ={
    val tableName = "MyTestTable"
    val columnIdentifier = "f1:c1"

    val conf = new SparkConf().setAppName("My Net Stream Testing")
      .setMaster("local[2]")
      .setSparkHome("/home/kufu/spark/spark-1.5.2-bin-hadoop2.6")
      .setJars(List("target/scala-2.10/my-net-stream-testing_2.10-1.0.jar"))

    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set(TableInputFormat.INPUT_TABLE, tableName)
    hbaseConfig.set(TableInputFormat.SCAN_COLUMNS, columnIdentifier)

    val ssc = new StreamingContext(conf, Seconds(2))

    val streamData = ssc.sparkContext.newAPIHadoopRDD(hbaseConfig, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
      .map(tuple=>tuple._2).map(result=>Bytes.toString(result.value()))

    val wordCounts = streamData.flatMap(_.split(" ")).map(word=>(word, 1)).reduceByKey(_+_)

    wordCounts.foreach(rdd=>{
      println("----------------")
      println(rdd._1+", "+rdd._2)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
