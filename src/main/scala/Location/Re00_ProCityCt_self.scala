package Location

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 统计省市指标
  */
object Re00_ProCityCt_self {

  def main(args: Array[String]): Unit = {

    if(args.length != 1){
      println("输入目录不正确")
      sys.exit()
    }
    val Array(inputPath) =args

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local[2]")
      //设置序列化级别
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // 获取数据
    val df = spark.read.parquet(inputPath)
    // 注册临时视图
    df.createTempView("log")
    val df2 = spark
      .sql("select count(*) ct,provincename,cityname  from log group by provincename,cityname")
    //按照省份，
    //df2.write.partitionBy("provincename","cityname").json("F:\\tmp\\json")
    //不按照省份
    //df2.write.json("F:\\tmp\\json3")
    //df2.show()


    // 存Mysql
    //way 1
/*
    df2.write.format("jdbc").mode("append")
      .option("url", "jdbc:mysql://localhost:3306/rdd")
      .option("dbtable", "table1")
      .option("user", "root")
      .option("password", "qq123321")
      .save()

*/

    //way 2
    // 通过config配置文件依赖进行加载相关的配置信息
    val load = ConfigFactory.load()
    // 创建Properties对象
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    // 存储
    df2.write.mode(SaveMode.Append).jdbc(
      load.getString("jdbc.url"),load.getString("jdbc.tablName"),prop)


    spark.stop()
  }
}
