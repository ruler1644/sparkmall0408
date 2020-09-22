package streamAPI

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


//TODO 不同位置程序代码执行次数不同
//TODO 行动算子外部定义的变量内部可用，若内部不可访问可将其做成广播变量
object Code_01_WordCount_01 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")
    val ssc = new StreamingContext(conf, Seconds(4))

    //封装参数
    val kafkaPara = Map(
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "zookeeper.connect" -> "hadoop102:2181,hadoop103:2181,hadoop104:2181",
      "group.id" -> "bigdata0408_WordCount"
    )

    //基于Direct方式消费kafka数据
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPara, Set("test"))

    //TODO Driver端，全局输出一次
    println(s"111${Thread.currentThread().getName}---------")

    //输出
    val DStream1: DStream[(String, Int)] = kafkaDStream.map(x => (x._2, 1)).transform(rdd => {

      //TODO Driver端，一个批次输出一次
      println(s"222${Thread.currentThread().getName}---------")

      val arr = Array(1, 2, 3, 4)

      rdd.foreach { x =>

        //TODO Executor端，一个批次输出多次
        println(s"333${Thread.currentThread().getName}---------")
        println(x)

        println(arr.length)
      }
      rdd
    })


    DStream1.print()

    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}
