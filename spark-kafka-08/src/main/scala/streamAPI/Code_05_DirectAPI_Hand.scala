//package streamAPI
//
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import org.apache.commons.codec.StringDecoder
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkConf, SparkContext}
//
////TODO 伪代码：StreamingAPI的DirectAPI方式，手动提交offset，
////数据消费处理与offset保存做成事务，保证消息的精准一次性，不需要设置checkpoint
//
//object Code_05_DirectAPI_Hand {
//  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("fds")
//    val ssc = new StreamingContext(conf, Seconds(3))
//
//
//    //使用Jdbc方式获取offset需要的参数，封装成partitionToLong对象．．．
//    val partitionToLong: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
//    val topicAndPartition = TopicAndPartition("aa", 1)
//    partitionToLong + (topicAndPartition -> 1000L)
//
//
//    //封装参数
//    val kafkaPara = Map(
//      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop102:9092",
//      "zookeeper.connect" -> "hadoop102:2181,hadoop103:2181,hadoop102:2181",
//      "group.id" -> "bigdata0408_04"
//    )
//
//    def messageHandler(m: MessageAndMetadata[String, String]): String = {
//      m.message()
//    }
//
//    //TODO 手动提交方式，需要fromOffsets信息，即partitionToLong
//    val kafkaDStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
//      ssc,
//      kafkaPara,
//      partitionToLong,
//      messageHandler
//    )
//
//    //多个主题，每个主题有多个分区
//    var offsetRanges = Array.empty[OffsetRange]
//
//    //TODO kafkaDStream转换操作----获取消费到的offset信息
//    val kafkaDStream2: DStream[String] = kafkaDStream.transform(
//      rdd => {
//        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//        rdd
//      })
//
//    //操作完成后，在行动算子中保存offset
//    kafkaDStream2.foreachRDD(rdd => {
//
//      //操作RDD．．．
//
//      for (elem <- offsetRanges) {
//        val topic: String = elem.topic
//        val partition: Int = elem.partition
//        val offset: Long = elem.untilOffset
//
//        //使用jdbc将topic、partition、offset信息写入Mysql数据库．．．
//      }
//    })
//
//    //打印
//    kafkaDStream.print()
//
//    //启动
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
