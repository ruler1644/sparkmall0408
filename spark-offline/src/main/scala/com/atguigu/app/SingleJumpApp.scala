package com.atguigu.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.datamode.UserVisitAction
import com.atguigu.handle.SingleJumpHandler
import com.atguigu.utils.{JdbcUtil, PropertiesUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SingleJumpApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("h")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    //读取配置文件,并获取过滤条件targetPageFlow：1，2，3，4，5，6，7
    val conditionStr: String = PropertiesUtil.load("conditions.properties").getProperty("condition.params.json")
    val jsonObj: JSONObject = JSON.parseObject(conditionStr)
    val targetPageFlowStr: String = jsonObj.getString("targetPageFlow")
    val targetPageFlowArr: Array[String] = targetPageFlowStr.split(",")

    //处理数据，取得过滤条件：1-2，2-3，3-4，4-5，5-6，6-7
    //去尾1,2,3,4,5,6
    val fromPages: Array[String] = targetPageFlowArr.dropRight(1)

    //掐头2,3,4,5,6,7
    val toPages: Array[String] = targetPageFlowArr.drop(1)

    //获取目标跳转页
    val tuples: Array[(String, String)] = fromPages.zip(toPages)
    val singleJumpPages: Array[String] = tuples.map {
      case (from, to) => (s"$from-$to")
    }

    //读取数据，获取userVisitAction
    val userVisitActionRDD: RDD[UserVisitAction] = spark.sql("select * from user_visit_action").as[UserVisitAction].rdd

    //TODO 并行路径一：每个页面点击次数====>(1,13) (2,40) (3,70) ...  计算6个页面即可，不需要最后一个页面
    val singlePageSum: RDD[(String, Long)] = SingleJumpHandler.getSinglePageCount(userVisitActionRDD, fromPages)

    //TODO 并行路径二：单个页面跳转次数====>[([1-2,10)(2-3,30)(3-4,40)...]
    val singleJumpCount: RDD[(String, Int)] = SingleJumpHandler.getSingleJumpCount(userVisitActionRDD, singleJumpPages)

    //拉取数据到Driver端
    val singlePageSumMap: Map[String, Long] = singlePageSum.collect().toMap
    val singleJumpCountArr: Array[(String, Int)] = singleJumpCount.collect()

    //计算跳转率
    val result: Array[Array[Any]] = singleJumpCountArr.map {
      case (singleJump, count) => {
        val singleJumpRatio: Double = count.toDouble / singlePageSumMap.getOrElse(singleJump.split("-")(0), 1L)

        //写入MySQL，准备参数
        Array(s"aaa--${System.currentTimeMillis()}", singleJump, singleJumpRatio)
      }
    }

    //写入MySQL
    JdbcUtil.executeBatchUpdate("insert into jump_page_ratio values(?,?,?)", result)
    spark.stop()
  }
}
