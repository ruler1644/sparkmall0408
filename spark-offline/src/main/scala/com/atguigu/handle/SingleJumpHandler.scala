package com.atguigu.handle

import com.atguigu.datamode.UserVisitAction
import org.apache.spark.rdd.RDD

object SingleJumpHandler {

  //获取单跳的次数,需要排序
  def getSingleJumpCount(userVisitActionRDD: RDD[UserVisitAction], singleJumpPages: Array[String]): RDD[(String, Int)] = {

    //转换数据格式userVisitAction---->[sessionId,(time,pageId)]
    val sessionToTimeAndPage: RDD[(String, (String, String))] = userVisitActionRDD.map(userVisitAction =>
      (userVisitAction.session_id, (userVisitAction.action_time, userVisitAction.page_id.toString))
    )

    //按照session分组,[sessionId,(time,pageId),(time2,pageId),(time3,pageId)]
    val sessionToTimeAndPageItr: RDD[(String, Iterable[(String, String)])] = sessionToTimeAndPage.groupByKey()

    //TODO 按时间升序排序,[sessionId,(time,pageId),(time2,pageId),(time3,pageId)]
    val sortedSessionToTimeAndPageItr: RDD[(String, List[(String, String)])] = sessionToTimeAndPageItr.mapValues(
      items => items.toList.sortWith(_._1 < _._1)
    )

    //过滤，1，5，6，18，2，7
    val filterSessionToPageItr: RDD[(String, List[String])] = sortedSessionToTimeAndPageItr.mapValues(items => {

      //a.去尾，1，5，6，18，2
      val fromPages: List[String] = items.map(_._2).dropRight(1)

      //b.掐头，5，6，18，2，7
      val toPages: List[String] = items.map(_._2).drop(1)

      //c.zip拉链,1-5,5-6,6-18,18-2,2-7
      val jumpPages: List[String] = fromPages.zip(toPages).map {
        case (from, to) => s"$from-$to"
      }

      //d.过滤,1-5,5-6,2-7
      jumpPages.filter(singleJumpPages.contains(_))
    })

    //(e5d8a7f9-6c41-478a-a8de-7257b6e4e2d7,List())
    //(2e3cc6a6-05ed-4ba7-8bb5-ad98698de4d1,List(3-4))
    //filterSessionToPageItr.collect().foreach(println)
    //println("********************")
    //filterSessionToPageItr.flatMap(_._2).collect().foreach(println)
    //println("--------------------")
    //filterSessionToPageItr.flatMap(_._2).map((_, 1)).collect().foreach(println)
    //println("====================")

    //扁平化，map后求和
    val singleJumpCount: RDD[(String, Int)] = filterSessionToPageItr.flatMap(_._2).map((_, 1)).reduceByKey(_ + _)

    //返回
    singleJumpCount
  }


  //获取指定页面的总的访问次数
  def getSinglePageCount(userVisitActionRDD: RDD[UserVisitAction], fromPages: Array[String]): RDD[(String, Long)] = {

    //过滤数据集，page_id位于，list(1,2,3,4,5,6)
    val filterUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(
      UserVisitAction => fromPages.contains(UserVisitAction.page_id.toString)
    )

    //转换格式：userVisitAction=>(page,1)
    val singlePageToOne: RDD[(String, Long)] = filterUserVisitActionRDD.map(
      userVisitAction => (userVisitAction.page_id.toString, 1L)
    )

    //求和
    val singlePageToSum: RDD[(String, Long)] = singlePageToOne.reduceByKey(_ + _)

    //返回
    singlePageToSum
  }
}
