package streamAPI

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

//TODO 基于Direct方式消费kafka数据，没有设置checkpoint
object Code_02_DirectAPI {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ds")
    val ssc = new StreamingContext(conf, Seconds(2))

    //封装参数
    val kafkaPara = Map(
      "bootstrap.servers" -> "hadoop102:9092,hadoop102:9092,hadoop102:9092",
      "zookeeper.connect" -> "hadoop102:2181,hadoop103:2181,hadoop104:2181",
      "group.id" -> "bigdata0409_02"
    )

    //StreamingContext，kafka参数，topic共三个参数
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPara, Set("test"))

    //打印数据
    kafkaDStream.map(_._2).print()

    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}
