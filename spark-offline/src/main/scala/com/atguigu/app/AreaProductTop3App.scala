package com.atguigu.app

import java.util.Properties

import com.atguigu.utils.PropertiesUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import udf.CityRatioUDAF

object AreaProductTop3App {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("top3")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    //注册UDAF函数
    spark.udf.register("myCityRatioUDAF", new CityRatioUDAF)

    //读取数据
    //|  华中|       商品_26|       长沙|
    //|  华东|       商品_88|       南京|
    val sql: String = "select c.area,p.product_name,c.city_name from user_visit_action u join city_info c on u.city_id = c.city_id join product_info p on u.click_product_id = p.product_id "
    spark.sql(sql).createOrReplaceTempView("area_product_tmp")

    //计算点击总数
    //|  华东|       商品_53|140|
    //|  华东|       商品_72|150|
    //spark.sql("select area,product_name,count(*) as ct from area_product_tmp group by area,product_name ").show(10)

    //计算点击总数及城市占比
    //|华东  |商品_53       |140|南京:16.4%,杭州:15.0%,其它：68.6% |
    //|华东  |商品_72       |150|无锡:18.0%,济南:17.3%,其它：64.7% |
    spark.sql("select area,product_name,count(*) as ct,myCityRatioUDAF(city_name) cityRatio from area_product_tmp group by area,product_name ").createOrReplaceTempView("area_product_count_tmp")

    //排名(rank排名时，分数相同名次相同，即有并列的情况，并且总数不会减少)
    //按照大区分组，按商品点击次数降序排序
    //|华东  |商品_89       |191|无锡:17.3%,苏州:15.7%,其它：67.0%|1                                                                                                           |
    //|华东  |商品_51       |188|杭州:17.0%,济南:16.5%,其它：66.5%|2                                                                                                           |
    //|华东  |商品_99       |185|南京:16.8%,上海:16.2%,其它：67.0%|3                                                                                                           |
    //|华东  |商品_63       |184|杭州:19.0%,无锡:16.8%,其它：64.2%|4                                                                                                           |
    //|华东  |商品_7        |184|无锡:17.9%,青岛:17.4%,其它：64.7%|4

    spark.sql(s"select area,product_name,ct,cityRatio,rank() over(partition by area order by ct desc) rk from  area_product_count_tmp").createOrReplaceTempView("area_product_count_rk_tmp")


    //取点击次数前三的商品（因为存在并列情况，所以记录数目可能大于3）
    val df: DataFrame = spark.sql(s"select 'aaa--${System.currentTimeMillis()}' task_id ,area,product_name,ct product_count,cityRatio city_click_ratio from  area_product_count_rk_tmp where rk <=3")

    //获取jdbc配置参数
    val properties: Properties = PropertiesUtil.load("config.properties")

    //写入MySQL
    df.write.format("jdbc")
      .option("url", properties.getProperty("jdbc.url"))
      .option("user", properties.getProperty("jdbc.user"))
      .option("password", properties.getProperty("jdbc.password"))
      .option("dbtable", "area_count_info")
      .mode(SaveMode.Append)
      .save()

    //关闭
    spark.stop()
  }
}
