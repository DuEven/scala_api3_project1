package Tags

import Tags2.TagsAPP
import com.typesafe.config.{ConfigFactory}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import util.TagUtils

/**
  * @author Duyw
  *         Date:2019/09/24  07:14:58
  * @Version ：1.0
  * @description:
  *
  */
object TagsContext00_All2Hbase {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("All2Hbase").master("local[*]").getOrCreate()

    import spark.implicits._

    //调用HbseAPI
    val load  = ConfigFactory.load()
    // 获取表名
    val HbaseTableName = load.getString("HBASE.tableName")
    // 创建Hadoop任务
    val configuration = spark.sparkContext.hadoopConfiguration
    // 配置Hbase连接
    configuration.set("hbase.zookeeper.quorum",load.getString("HBASE.Host"))
    // 获取connection连接
    val hbConn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbConn.getAdmin


    // 判断当前表是否被使用
    if(!hbadmin.tableExists(TableName.valueOf(HbaseTableName))){
      println("当前表可用")
      // 创建 表 对象
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(HbaseTableName))
      // 创建列簇
      val hColumnDescriptor = new HColumnDescriptor("tags")
      // 将创建好的列簇加入表中
      tableDescriptor.addFamily(hColumnDescriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbConn.close()
    }

    val conf = new JobConf(configuration)
    // 指定输出类型
    conf.setOutputFormat(classOf[TableOutputFormat])
    // 指定输出哪张表
    conf.set(TableOutputFormat.OUTPUT_TABLE,HbaseTableName)



    val df: DataFrame = spark.read.parquet("F:\\tmp\\output4s_self")
    val docsRDD: collection.Map[String, String] = spark.sparkContext
      .textFile("F:\\tmp\\app_dict.txt").map(_.split("\\s"))
      .filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()
    //广播字典
    val broadValue: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(docsRDD)

    // 读取停用词典
    val stopwordsRDD = spark.sparkContext.textFile("F:tmp\\stopwords.txt").map((_,0)).collectAsMap()
    // 广播字典
    val broadValues = spark.sparkContext.broadcast(stopwordsRDD)

    // 处理数据信息
    df.map(row=>{
      // 获取用户的唯一ID
      val userId:String = TagUtils.getOneUserId(row)
      // 接下来标签 实现
      val adList = TagsAd.makeTags(row)
      // 商圈
      val businessList = BusinessTag.makeTags(row)
      // 媒体标签
      val appList = TagsAPP.makeTags(row,broadValue)
      // 设备标签
      val devList = TagsDevice.makeTags(row)
      // 地域标签
      val locList = TagsLocation.makeTags(row)
      // 关键字标签
      val kwList = TagsKword.makeTags(row,broadValues)

      (userId,adList++appList++businessList++devList++locList++kwList)
    })
      .rdd.reduceByKey((list1,list2)=>{
      (list1:::list2)
        .groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_+_._2))
        .toList
    })
      .map{
        case(userId,userTags) =>{
          //设置rowkey和列，列名
          val put = new Put(Bytes.toBytes(userId))

          put.addImmutable(Bytes.toBytes("tags"),
            Bytes.toBytes(20190924),
            Bytes.toBytes(userTags.mkString(","))
          )
          (new ImmutableBytesWritable(),put)
        }
      }.saveAsHadoopDataset(conf)


  }

}
