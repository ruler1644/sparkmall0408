package app

import bean.AdsLog
import com.atguigu.utils.MyKafkaUtil
import handler.{BlackListHandler, DateAreaCityAdCountHandler, LastHourAdClickHandler, Top3DateAreaAdCountHandler}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimeAPP {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dkl")
    val ssc = new StreamingContext(conf, Seconds(3))

    //用到有状态的转换，需要设置检查点
    ssc.sparkContext.setCheckpointDir("./ck0408")

    //读取kafka数据
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, "ads_log")

    //将数据集内容转换为样例类对象(date,area,city,userid,adid)
    val adsLogDStream: DStream[AdsLog] = kafkaDStream.map {
      case (_, value) => {
        val splits: Array[String] = value.split(" ")
        AdsLog(splits(0).toLong, splits(1), splits(2), splits(3), splits(4))
      }
    }

    //TODO　需求五：a.根据黑名单过滤数据集
    val filterAdsLogDStream: DStream[AdsLog] = BlackListHandler.filterDataByBlackList(ssc.sparkContext, adsLogDStream)
    filterAdsLogDStream.cache()

    //TODO 需求六：获取每天每个大区各个城市广告点击总数，并将其写入Redis
    val dateAreaCityToCount: DStream[((String, String, String, String), Long)] = DateAreaCityAdCountHandler.getDateAreaCityCount(filterAdsLogDStream)
    DateAreaCityAdCountHandler.saveDateAreaCityAdCountToRedis(dateAreaCityToCount)
    dateAreaCityToCount.cache()

    //TODO 需求七：每天各地区热门广告Top3，并将其写入Redis
    Top3DateAreaAdCountHandler.saveDateAreaTop3AdCountToRedis(dateAreaCityToCount)

    //TODO 需求八：最近一小时每分钟广告点击量
    LastHourAdClickHandler.saveLastHourAdCountToRedis(filterAdsLogDStream)

    //TODO　需求五：b.校验点击次数，点击次数超过100加入黑名单
    BlackListHandler.checkDataToBackList(filterAdsLogDStream)

    ssc.start()
    ssc.awaitTermination()
  }
}