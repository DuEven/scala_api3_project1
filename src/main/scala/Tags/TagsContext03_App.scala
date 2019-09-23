package Tags

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import util.TagUtils

/**
  * 上下文标签主类
  */
object TagsContext03_App {

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


    // todo 读取数据字典  //s 切空格或者Tab键
    val docMap: collection.Map[String, String] = spark.sparkContext.textFile("F:\\tmp\\app_dict.txt")
      .map(_.split("\\s", -1))
      .filter(_.length >= 5)
      .map(arr => (arr(4), arr(1)))
      .collect().toMap
    // todo    .collectAsMap()   <==>  .collect().toMap

    // todo 进行广播 将Driver端的数据分批发送放Excutor减少网络IO，节约内存
    val broadcast: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(docMap)



    // 处理数据信息
   // df.map((_,1)).rdd

    val userId2AdTagRDD = df.map(row => {
      // 获取用户的唯一ID
      val userId = TagUtils.getOneUserId(row)
      // 接下来标签 实现
      val appList = TagsApp.makeTags(row,broadcast)

       (userId, appList)

    }).rdd

    userId2AdTagRDD.foreach(println)
    //userId2AdTagRDD.saveAsTextFile("F:\\tmp\\AppTags")

  }
}
