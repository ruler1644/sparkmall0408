package com.atguigu.accu

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class CategoryAccu extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  private var categoryCount = new mutable.HashMap[String, Long]()

  //判空
  override def isZero: Boolean = {
    categoryCount.isEmpty
  }

  //复制
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accu = new CategoryAccu

    //list1 ++= list，表示的是将集合list中的各个元素加入集合list1(list1 = list1 ++ list0)
    accu.categoryCount ++= this.categoryCount
    accu
  }

  //重置
  override def reset(): Unit = {
    categoryCount.clear()
  }

  //分区内添加或修改单个值====>给对应得key加1
  override def add(key: String): Unit = {
    categoryCount(key) = categoryCount.getOrElse(key, 0L) + 1L
  }

  //分区间合并数据
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {

    //获取other的value
    val otherCategoryCount: mutable.HashMap[String, Long] = other.value

    otherCategoryCount.foreach {
      case (category, count) => {
        this.categoryCount(category) = this.categoryCount.getOrElse(category, 0L) + count
      }
    }
  }

  override def value: mutable.HashMap[String, Long] = {
    categoryCount
  }

}


class MyAccumulator1 extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  var wordMap = mutable.HashMap[String, Long]()

  override def isZero: Boolean = {
    wordMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new MyAccumulator1
  }

  override def reset(): Unit = {
    wordMap.clear()
  }

  override def add(key: String): Unit = {

    //wordMap.update(key, 1)
    wordMap(key) = wordMap.getOrElse(key, 0L) + 1L
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {

    //wordMap
    //otherWordMap
    val map1: mutable.HashMap[String, Long] = wordMap
    val map2: mutable.HashMap[String, Long] = other.value

    wordMap = map1.foldLeft(map2)(
      (map, kv) => {
        map(kv._1) = map.getOrElse(kv._1, 0L) + kv._2
        map
      }
    )
  }

  override def value: mutable.HashMap[String, Long] = {
    wordMap
  }
}