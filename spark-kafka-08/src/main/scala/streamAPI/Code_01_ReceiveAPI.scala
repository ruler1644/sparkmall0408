package streamAPI

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}


//TODO 老版本，Streaming的ReceiveAPI方式，将offset信息保存在zookeeper上
object Code_01_ReceiveAPI {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ds")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    //连接Kafka的参数
    val kafkaPara = Map(
      "zookeeper.connect" -> "hadoop102:2181,hadoop103:2181,hadoop104:2181",
      "group.id" -> "bigdata0409"
    )

    val topics = Map("test" -> 1)

    //KafkaUtils直接创建Stream，默认是Receiver方式
    //StreamingContext,kafka参数，topic,存储级别四个参数
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPara, topics, StorageLevel.MEMORY_ONLY)

    //打印数据
    kafkaDStream.map(_._2).print()

    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}
