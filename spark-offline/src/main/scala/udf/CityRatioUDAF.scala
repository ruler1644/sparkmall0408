package udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

//(北京，1),1L
//[(北京，1),(石家庄，1),2L]
class CityRatioUDAF extends UserDefinedAggregateFunction {

  //输入数据类型
  override def inputSchema: StructType = StructType(StructField("city", StringType) :: Nil)

  //缓冲区数据类型
  override def bufferSchema: StructType = StructType(
    StructField("cityCount", MapType(StringType, LongType)) :: StructField("totalCount", LongType) :: Nil
  )

  //输出数据类型
  override def dataType: DataType = StringType

  //函数稳定型(相同的输入是否应该返回相同的输出)
  override def deterministic: Boolean = true

  //缓存初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
    buffer(1) = 0L
  }

  //区内更新数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    //取出缓冲区buffer中数据
    val cityCount: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalCount: Long = buffer.getLong(1)

    val cityName = input.getString(0)

    //赋值
    buffer(0) = cityCount + (cityName -> (cityCount.getOrElse(cityName, 0L) + 1L))
    buffer(1) = totalCount + 1L
  }

  //分区间聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    //取出缓冲区中的值
    val cityCount1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
    val totalCount1: Long = buffer1.getLong(1)

    val cityCount2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)
    val totalCount2: Long = buffer2.getLong(1)

    //赋值(合并两个map)
    buffer1(0) = cityCount1.foldLeft(cityCount2) {
      case (map, (city, count)) => {
        map + (city -> (map.getOrElse(city, 0L) + count))
      }
    }

    //总量的合并
    buffer1(1) = totalCount1 + totalCount2
  }

  //计算最终结果：北京21.2%,天津13.2%,其他65.6%
  override def evaluate(buffer: Row): String = {

    //取值
    val cityCount: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalCount: Long = buffer.getLong(1)
    var otherRadio = 100D

    //对城市点击次数进行降序排序，并取出前两名
    val sortedCityCount: List[(String, Long)] = cityCount.toList.sortWith(_._2 > _._2).take(2)

    //计算城市占比
    val cityRatioTop2: List[String] = sortedCityCount.map {
      case (city, count) =>
        val ratio: Double = Math.round(count.toDouble * 1000 / totalCount) / 10D
        otherRadio -= ratio
        s"$city:$ratio%"
    }

    //其他城市
    val cityRatio: List[String] = cityRatioTop2 :+ s"其它：${Math.round(otherRadio * 10) / 10D}%"

    //将集合List转化为字符串String
    cityRatio.mkString(",")
  }
}