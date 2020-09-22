package DirectAPI

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

//TODO kafka-0-10版本，Streaming只支持DirectAPI，
//offset默认自动保存在kafka内置主题__consumer_offsets
object Code_01_DirectAPI {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("fg")
    val ssc = new StreamingContext(conf, Seconds(2))

    //封装参数,需要反序列化Key和Value
    val kafkaPara = Map(
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop102:9092",
      "zookeeper.connect" -> "hadoop102:2181,hadoop103:2181,hadoop102:2181",
      "group.id" -> "bigdata0408_05",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    //新版本Direct方式消费kafka中数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,

      //TODO 生产策略和消费策略
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array("test"), kafkaPara)
    )

    //打印数据，ConsumerRecord不可序列化
    kafkaDStream.map(record => record.value()).print()

    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}
