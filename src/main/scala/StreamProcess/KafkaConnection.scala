package StreamProcess

import java.util

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, Producer}
/**
  * Created by kufu on 16-1-26.
  */
class KafkaConnection(producerList:List[String], topic:String) {

  var producer:Producer[String, String] = {

    val props = new util.Properties()
    val list:util.ArrayList[String] = new util.ArrayList[String]()
    producerList.foreach(location=>list.add(location))
    props.put("bootstrap.servers", list)

    //details configuration
    props.put("acks", "all")
    props.put("retries", new Integer(0))
    props.put("batch.size", new Integer(16384))
    props.put("linger.ms", new Integer(1))
    props.put("buffer.memory", new Integer(33554432))

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer(props)
  }

  def send(message:String): Unit ={
    val key = "timestamp:"+System.currentTimeMillis()
    producer.send(new ProducerRecord[String, String](topic, key, message))
  }

  def close(): Unit ={
    producer.close()
  }
}
