import java.util
import java.util.concurrent._

import kafka.api._
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}

import scala.collection.JavaConversions._

/**
  * Created by kufu on 16-1-19.
  */
class DirectConnectionToKafka {

  private val debug = true

  def produce(): Unit ={
    val props = new util.Properties()
    val list:util.ArrayList[String] = new util.ArrayList[String]()
    list.add("localhost:9101")
    list.add("localhost:9102")
    list.add("localhost:9103")
    props.put("bootstrap.servers", list)

    //details configuration
    props.put("acks", "all")
    props.put("retries", new Integer(0))
    props.put("batch.size", new Integer(16384))
    props.put("linger.ms", new Integer(1))
    props.put("buffer.memory", new Integer(33554432))

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer:Producer[String, String] = new KafkaProducer(props)
    for(i<- 0 until 10){
      producer.send(new ProducerRecord[String, String]("myTopic", "key:"+i, "NewValue:"+i))
    }
    producer.close()

  }

  val m_replicaBrokers:mutable.HashSet[String] = new mutable.HashSet[String]()

  /**
    *
    * @param seedBrokers any available brokers list
    * @param port available broker port
    * @param topic topic concerned
    * @return It's broker.Id, broker.host, broker.port, partition.Id in tuple4
    */
  def findLeader(seedBrokers:List[String], port:Int, topic:String):List[(Int, String, Int, Int)]={
    var retMetaData = new ListBuffer[(Int, String, Int, Int)]()
    var partitionId: Int = -1
    val checkSet:mutable.HashSet[String] = new mutable.HashSet[String]()
    var replicaIsDirt = true
    seedBrokers.foreach(seed=>{
      var consumer:SimpleConsumer = null
      try{
        //an id to match request and response
        val correlationId:Int = System.currentTimeMillis.toInt

        consumer = new SimpleConsumer(seed, port, 100000, 64*1024, "leaderLookup")

        val topics:util.List[String] = util.Collections.singletonList[String](topic)
        val request = new TopicMetadataRequest(topics, correlationId)
        val response:TopicMetadataResponse = consumer.send(request)

        val metadata:Seq[TopicMetadata] = response.topicsMetadata

        metadata.foreach(item=>{
          item.partitionsMetadata.foreach(partitionData=>{
            partitionId = partitionData.partitionId
            val broker = partitionData.leader.get
            if(debug){
              println("---------------------")
              println("host:"+broker.host)
              println("id:"+broker.id)
              println("port:"+broker.port)
              println("connection:"+broker.connectionString)
              println("partitionId:"+partitionId)
            }
            val addr = broker.host+":"+broker.id
            if(!checkSet.contains(addr)){
              retMetaData+=((broker.id, broker.host, broker.port, partitionId))
              checkSet.add(addr)
            }
            partitionData.replicas.foreach(replica=>{
              if(replicaIsDirt){
                m_replicaBrokers.clear()
                replicaIsDirt = false
              }
              m_replicaBrokers.add(replica.host)
            })
          })
        })
      } catch {
        case e: Exception => println("Error communicating with Broker["+seed
            +"] to find leader for ["+topic+", "+partitionId+"]")
      } finally {
        if(consumer!=null) consumer.close()
      }
    })

    retMetaData.toList
  }

  def findNewLeader(oldLeadBroker: String, aTopic: String, aPartition: Int, aPort: Int): String = {
    var ret = ""
    if((0 until 3).exists(i=>{
      val brokerList = findLeader(m_replicaBrokers.toList, aPort, aTopic)

      if(!brokerList.exists(tuple=>{
        ret = tuple._2
        tuple._4==aPartition && !oldLeadBroker.equalsIgnoreCase(tuple._2)
      })) {
        Thread.sleep(1000)
        false
      } else true

    })) ret
    else{
      println("Unable to find new leader")
      throw new Exception("Unable to find new leader")
    }
  }

  def simpleConsume(aMaxRead:Long,
                    aTopic:String,
                    aPartition:mutable.HashSet[Int],
                    aSeedBrokers:List[String],
                    aPort:Int): Unit ={
    val metaData = findLeader(aSeedBrokers, aPort, aTopic)

    if (metaData==null || metaData.isEmpty) {
      println("Can't find metadata for topic and partition")
    } else {
      val consumeListBuffer = new ListBuffer[SimpleConsume]
      metaData.foreach(tuple=>{
        if(aPartition==null || aPartition.contains(tuple._4))
          consumeListBuffer+=new SimpleConsume(tuple._2, tuple._3, tuple._4, aTopic, aMaxRead, 0)
      })
      val consumeList = consumeListBuffer.toList
      var consumeNum = consumeList.size
      val executor = Executors.newFixedThreadPool(consumeNum)
      val pool = new ExecutorCompletionService[Feedback](executor)
      consumeList.foreach(pool.submit(_))

      while(consumeNum>0){
        val feedback:Feedback = pool.take().get()
          feedback.getCode match {
          case SimpleConsume.Complete => consumeNum-=1

          case SimpleConsume.ToFindNew =>
            val newLeadBroker = findNewLeader(feedback.getHost,
              feedback.getTopic,
              feedback.getPartition,
              feedback.getPort)
            pool.submit(new SimpleConsume(newLeadBroker,
              feedback.getPort,
              feedback.getPartition,
              feedback.getTopic,
              aMaxRead,
              feedback.getErrNum))

          case SimpleConsume.TooManyErrs => consumeNum-=1
        }
      }

    }
  }

  def simpleConsume(): Unit ={
    simpleConsume(10000, "myTopic", null, "localhost"::Nil, 9101)
  }

  def consume(): Unit ={
    val props = new util.Properties()
    props.put("bootstrap.servers", "localhost:9093")

    //details configuration
    props.put("group.id", "test")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer:KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

//    while(true){
      for( records:ConsumerRecords[String, String] <- consumer.poll(100).values())
        for(record<-records.records()){
          println("offset:"+record.offset()+", key:"+record.key()+", value:"+record.value())
        }
//    }
    consumer.close()
  }

  def highlevelConsume(numThread:Int): Unit ={
    def getConsumer: ConsumerConfig ={
      val props = new util.Properties()

      //details configuration
      props.put("zookeeper.connect", "localhost:2181")
      props.put("group.id", "test")
      props.put("zookeeper.session.timeout.ms", "400")
      props.put("zookeeper.sync.time.ms", "200")
      props.put("auto.commit.interval.ms", "1000")

      new ConsumerConfig(props)
    }
    val connector:ConsumerConnector = Consumer.create(getConsumer)

    val topic = "myTopic"
    val topicCountMap = Map[String, Int](topic->5)

    val streams = connector.createMessageStreams(topicCountMap)
    val streamKafka = streams.get(topic)

    println("stream size:"+streamKafka.get.size)
    val executor = Executors.newFixedThreadPool(streamKafka.get.size)

    var threadNum=0
    for(stream<-streamKafka.get){
      executor.execute(new HighlevelConsume(stream, threadNum))
      threadNum+=1
    }
  }

}
class HighlevelConsume(stream:KafkaStream[Array[Byte], Array[Byte]], threadNum:Int) extends Runnable {
  override def run(): Unit = {
    val it:ConsumerIterator[Array[Byte], Array[Byte]] = stream.iterator()
    while(it.hasNext()){
      println("Thread "+threadNum+": " + new java.lang.String(it.next().message()))
    }
  }
}

class Feedback(code:Int, brokerHost:String, brokerPort:Int, partition:Int, topic:String, errNum:Int){
  def getCode = code
  def getHost = brokerHost
  def getPort = brokerPort
  def getPartition = partition
  def getTopic = topic
  def getErrNum = errNum
  def this(code:Int){
    this(code, null, -1, -1, null, 0)
  }
}

object SimpleConsume{
  val ToFindNew = 1
  val Complete = 0
  val TooManyErrs = 2
}
class SimpleConsume(brokerHost:String,
                    brokerPort:Int,
                    partition:Int,
                    topic:String,
                    maxRead:Long,
                    errNumber:Int)extends Callable[Feedback] {

  override def call(): Feedback = {
    val clientName = "Client_"+topic+"_"+partition

    var simpleConsumer = new SimpleConsumer(brokerHost, brokerPort, 10000, 64*1024, clientName)
    var readOffset = getLastOffset(simpleConsumer, topic, partition,
      OffsetRequest.EarliestTime, clientName)
    var curRead = 0
    var errNum = errNumber
    while(curRead<maxRead && errNum<5){
      val fetchRequest = new FetchRequestBuilder()
        .clientId(clientName).addFetch(topic, partition, readOffset, 1000000).build()

      val fetchResponse = simpleConsumer.fetch(fetchRequest)
      if(fetchResponse.hasError){
        val code = fetchResponse.errorCode(topic, partition)
        println("fetch error with code " + code)
        errNum+=1
        if(errNum>5){return new Feedback(SimpleConsume.TooManyErrs)}
        if(code==ErrorMapping.OffsetOutOfRangeCode){
          readOffset = getLastOffset(simpleConsumer,
            topic, partition, OffsetRequest.LatestTime, clientName)
        }else{
          simpleConsumer.close()
          simpleConsumer = null

          return new Feedback(SimpleConsume.ToFindNew, brokerHost, brokerPort, partition, topic, errNum)
        }
      }else{
        var tmpRead = 0
        for(message <- fetchResponse.messageSet(topic, partition)){
          errNum = 0
          val curOffset = message.offset
          if(curOffset<readOffset){
            println("found an old offset: ")
          }else{
            readOffset = message.nextOffset
            val payload = message.message.payload
            val bytes = new Array[Byte](payload.limit())
            payload.get(bytes)
            println("Topic:"+topic+", Partition:"+partition+", Value:"+ new String(bytes))
            curRead+=1
            tmpRead+=1
          }
          if(tmpRead==0){
            Thread.sleep(1000)
          }
        }
      }
    }
    if(simpleConsumer != null){
      simpleConsumer.close()
    }
    new Feedback(SimpleConsume.Complete)
  }

  def getLastOffset(simpleConsumer: SimpleConsumer,
                    topic:String,
                    partition:Int,
                    whichTime:Long,
                    clientName:String
                   ): Long ={
    val topicAndPartition:TopicAndPartition = new TopicAndPartition(topic, partition)
    val requestInfo = Map(topicAndPartition->new PartitionOffsetRequestInfo(whichTime, 1))
    val request = new OffsetRequest(requestInfo, OffsetRequest.CurrentVersion, 0,
      clientName, Request.OrdinaryConsumerId)
    val response = simpleConsumer.getOffsetsBefore(request)

    if(response.hasError){
      println("Error fetching data offset data from broker.")
      0
    }else{
      val offsets = response.offsetsGroupedByTopic.get(topic).get
        .get(topicAndPartition).get.offsets
      offsets.head
    }
  }
}