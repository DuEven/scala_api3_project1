package exam0922

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * @author 杜逸文
  *         Date:2019/09/22  10:54:19
  * @Version ：1.0
  * @description:
  *
  */
object test2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("test2").master("local").getOrCreate()

    val lineRDD: RDD[String] = spark.sparkContext.textFile("in\\json.txt")

    val mapRDD: RDD[String] = lineRDD.map(x =>JsonUtil.getBusiness(x,"type"))

    val res2 = mapRDD.map(line => {
      val str: Array[String] = line.split(";")
      var list = List[(String, Int)]()
      for (ele <- str) {
        list :+= (ele, 1)
      }

      list.groupBy(_._1).mapValues(_.size).toList
    })

/*    var list =List[(String,Int)]()
    val res2: RDD[(String, Int)] = mapRDD.flatMap(_.split(",")).map {
      v => ("TYPE" + v, 1)
    }*/
    res2.foreach(println)


    spark.stop()


  }


}
