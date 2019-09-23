package exam0922

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}



/**
  * @author 杜逸文
  *         Date:2019/09/22  10:54:19
  * @Version ：1.0
  * @description:
  *
  */
object test1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("test1").master("local").getOrCreate()
    import spark.implicits._

    val lineRDD: RDD[String] = spark.sparkContext.textFile("in\\json.txt")

    val mapRDD = lineRDD.map(x => JsonUtil.getBusiness(x,"businessarea"))


    val res1  = mapRDD.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)

    res1.foreach(println)

    spark.stop()


  }



}
