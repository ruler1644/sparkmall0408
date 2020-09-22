package streamAPI

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//TODO 读取checkPoint目录下offset信息,创建StreamingContext,保证从offset向下继续消费
object Code_04_DirectAPI_ck2 {

  def getStreamingContext(ck: String): StreamingContext = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ds")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    //TODO 设置checkPoint
    ssc.checkpoint(ck)

    //封装参数
    val kafkaPara = Map(
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "zookeeper.connect" -> "hadoop102:2181,hadoop103:2181,hadoop104:2181",
      "group.id" -> "bigdata0408_03"
    )

    //基于Direct方式消费kafka数据
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPara, Set("test"))

    //打印数据
    kafkaDStream.map(_._2).print()

    //返回ssc
    ssc
  }

  def main(args: Array[String]): Unit = {
    val ck = "./checkPoint_02"

    //TODO 从CheckPoint恢复StreamingContext，做CheckPoint目录和恢复CheckPoint目录需要一致
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate(ck, () => getStreamingContext(ck))

    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}
