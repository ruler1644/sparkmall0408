package shutdown

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object GraceShutdown {

  def createSSC(): StreamingContext = {

    val update: (Seq[Int], Option[Int]) => Some[Int] = (values: Seq[Int], status: Option[Int]) => {

      //当前批次内容的计算
      val sum: Int = values.sum

      //取出状态信息中的上一次状态
      val lastStatus: Int = status.getOrElse(0)

      Some(sum + lastStatus)
    }

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("fg")

    //设置优雅的关闭
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./CheckPoint-Shutdown")

    //监听端口数据做有状态的wordCount
    val line: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    val word: DStream[String] = line.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = word.map((_, 1))
    val wordAndSum: DStream[(String, Int)] = wordAndOne.updateStateByKey(update)
    wordAndSum.print()
    ssc
  }

  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./CheckPoint-Shutdown", () => createSSC())
    new Thread(new MonitorStop(ssc)).start()
    ssc.start()
    ssc.awaitTermination()
  }
}
