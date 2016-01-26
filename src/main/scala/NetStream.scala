import StreamProcess.{HBaseConnection, StreamFromKafka}

object NetStream{
  def main(args: Array[String]): Unit = {
//    (new ReadFromSocket).run()
//    (new HBaseReading).run()
//    (new DirectConnectionToKafka).produce()
//    (new DirectConnectionToKafka).highlevelConsume(1)
//    (new SparkStreamingKafka).kafkaStream()
//    (new DirectConnectionToKafka).simpleConsume()
    (new StreamFromKafka).run
//    (new HBaseConnection("tn", null)).openOrCreateTable()
  }

}
