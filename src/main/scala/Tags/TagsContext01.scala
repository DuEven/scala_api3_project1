package Tags

import org.apache.spark.rdd.RDD
import util.TagUtils
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}

/**
  * 上下文标签主类
  */
object TagsContext01 {

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

    val userId2BusinessTagRDD = df.map(row => {
      // 获取用户的唯一ID
      val userId = TagUtils.getOneUserId(row)
      // 接下来标签 实现
      val adList = TagsAd.makeTags(row)
      // 商圈
      val businessList = BusinessTag.makeTags(row)
       //(userId, adList)
      (userId, businessList)
    }).rdd



    val userId2FilterBTagRDD: RDD[(String, List[(String, Int)])] = userId2BusinessTagRDD.filter {
      case (userId, listBusinessTag) =>
        listBusinessTag.size > 0
    }


   val userTagRDD: RDD[UserTag] = userId2FilterBTagRDD.map {
      case (userId, listBTag) =>
        var aggInfo: String = ""
        for (item <- listBTag) {
          val name = item._1
          val count = item._2
          aggInfo += name + "=" + count + "|"
        }
        if(aggInfo.endsWith("\\|")){
          aggInfo = aggInfo.substring(0, aggInfo.length - 1)
        }
        UserTag(userId, aggInfo)
    }
    userTagRDD.foreach(println(_))

  }
}
case class UserTag(userId:String, BTag:String )