package util

import Tags.{BusinessTag, TagsAd}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.SparkSession

/**
  * 测试工具类
  */
object Test {

/*  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    val spark = SparkSession.builder().appName("Tags").master("local[*]").getOrCreate()
    import spark.implicits._

    // 读取数据文件
    val df = spark.read.parquet("D:\\gp23dmp")
    df.map(row=>{
      // 圈
      AmapUtil.getBusinessFromAmap(
        String2Type.toDouble(row.getAs[String]("long")),
        String2Type.toDouble(row.getAs[String]("lat")))
    }).rdd.foreach(println)
  }*/
  def main(args: Array[String]): Unit = {

    var str:String = ""
    println(get(str))

  }

  def get(url:String):String={

    val client = HttpClients.createDefault()

    val httpGet = new HttpGet(url)
    // 发送请求
    val httpResponse = client.execute(httpGet)
    // 处理返回请求结果
    EntityUtils.toString(httpResponse.getEntity,"UTF-8")
  }


}
