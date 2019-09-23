package Tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import util.TagUtils

/**
  * 渠道 上下文标签主类
  */
object TagsContext04_Adplatformproviderid {

  def main(args: Array[String]): Unit = {
    if(args.length!=1){
      println("目录不正确")
      sys.exit()
    }
    val Array(inputPath)=args

    // 创建Spark上下文
    val spark = SparkSession.builder().appName("Tags").master("local").getOrCreate()
    import spark.implicits._

    // 读取数据文件
    val df: DataFrame = spark.read.parquet(inputPath)

    // 处理数据信息
   // df.map((_,1)).rdd

    val userId2AdTagRDD = df.map(row => {

      // 获取用户的唯一ID
      val userId = TagUtils.getOneUserId(row)
      // 接下来标签 实现
      val AdplatList = Adplatform(row)

       (userId, AdplatList)

    }).rdd
      .foreach(println)

    //userId2AdTagRDD.saveAsTextFile("F:\\tmp\\AppTags")

  }


  def Adplatform (args: Any*): List[(String, Int)] = {

    var list =List[(String,Int)]()
    // 获取数据类型
    val row = args(0).asInstanceOf[Row]
    // 获取广告位类型和名称
    val adType = row.getAs[Int]("adplatformproviderid")
    // 广告位类型标签
    adType match {
      case v  => list:+=("CN"+v,1)
    }
    list

  }
}
