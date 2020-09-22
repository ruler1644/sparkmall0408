package test

import org.json4s.jackson.JsonMethods

object ArrayToJson {
  def main(args: Array[String]): Unit = {

    val list = List(("1", 1), ("2", 2), ("3", 3))
    import org.json4s.JsonDSL._
    println(JsonMethods.compact(list))
  }

}
