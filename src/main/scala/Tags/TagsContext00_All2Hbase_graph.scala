package Tags

import Tags2._
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import util.TagUtils

/**
  * @author Duyw
  *         Date:2019/09/24  07:14:58
  * @Version ：1.0
  * @description:
  *
  */
object TagsContext00_All2Hbase_graph {
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

    val allUserId = df.rdd.map(row=>{
      // 获取所有ID
      val strList = TagUtils.getallUserId(row)
      (strList,row)
    })

    //todo 构建点集合
    val verties: RDD[(Long, List[(String, Int)])] = allUserId.flatMap(row => {
      // 获取所有数据
      val rows = row._2

      val adList = TagsAd.makeTags(rows)
      // 商圈
      val businessList = BusinessTag.makeTags(rows)
      // 媒体标签
      val appList = TagsAPP.makeTags(rows,broadValue)
      // 设备标签
      val devList = TagsDevice.makeTags(rows)
      // 地域标签
      val locList = TagsLocation.makeTags(rows)
      // 关键字标签
      val kwList = TagsKword.makeTags(rows,broadValues)
      // 获取所有的标签
      val tagList = adList++ appList++devList++locList++kwList
      // 保留用户Id
      val VD = row._1.map((_,0))++tagList
      // 思考  1. 如何保证其中一个ID携带着用户的标签
      //     2. 用户ID的字符串如何处理
      row._1.map(uId=>{
        if(row._1.head.equals(uId)){
          (uId.hashCode.toLong,VD)
        }else{
          (uId.hashCode.toLong,List.empty)
        }
      })
    })
    //打印
    verties.take(10).foreach(println(_))

    //todo 构建边集合
    val edges: RDD[Edge[Int]] = allUserId.flatMap(row => {
      //A B C :A->B  A->C
      row._1.map(uId => Edge(row._1.head.hashCode.toLong, uId.hashCode.toLong, 0))
    })
    edges.foreach(println)
    // 构建图
    val graph = Graph(verties,edges)

    // 根据图计算中的连通图算法，通过图中的分支，连通所有的点
    // 然后在根据所有点，找到内部最小的点，为当前的公共点
    val vertices = graph.connectedComponents().vertices
    // 聚合所有的标签
    vertices.join(verties).map{
      case (uid,(cnId,tagsAndUserId))=>{
        (cnId,tagsAndUserId)
      }
    }.reduceByKey(
      (list1,list2)=>{
        (list1++list2)
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)
          .toList
      }).map{
      case (userId,userTags) =>{
        // 设置rowkey和列、列名
        val put = new Put(Bytes.toBytes(userId))
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(20190924),Bytes.toBytes(userTags.mkString(",")))
        (new ImmutableBytesWritable(),put)
      }
    }.foreach(println)
      //.saveAsHadoopDataset(conf)

    spark.stop()

  }

}
