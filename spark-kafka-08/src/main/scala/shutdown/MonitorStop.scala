package shutdown

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}

class MonitorStop(ssc: StreamingContext) extends Runnable {
  override def run(): Unit = {

    val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), new Configuration(), "wu")


    while (true) {
      try {
        Thread.sleep(5000)
      } catch {
        case e: InterruptedException =>
          e.getMessage
      }

      //获取SSC状态
      val state: StreamingContextState = ssc.getState()

      //获取地址
      val bool: Boolean = fs.exists(new Path("hdfs://hadoop102:9000/StopSpark"))

      if (bool) {
        if (state == StreamingContextState.ACTIVE) {

          //每个Streaming中都有一个SParkContext
          //优雅：先关掉receiver不再接收数据,将已有数据处理完毕之后,再关闭Executor
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          System.exit(0)
        }
      }
    }
  }
}
