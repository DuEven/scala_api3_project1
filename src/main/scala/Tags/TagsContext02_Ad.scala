package Tags

import util.TagUtils
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}

/**
  * Ad 标签的上下文标签主类
  */
object TagsContext02_Ad {

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
      val adList = TagsAd.makeTags(row)
      // 商圈
      val businessList = BusinessTag.makeTags(row)
       (userId, adList)
      //(userId, businessList)
    }).rdd

    userId2AdTagRDD.foreach(println(_))

  }
}
