package test

import com.atguigu.utils.MyKafkaUtil
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TestKafkaConsumer {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dd")
    val ssc = new StreamingContext(conf, Seconds(5))

    //读取kafka数据
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, "ads_log")

    //打印数据
    kafkaDStream.map(_._2).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
