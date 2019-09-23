package Tags

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import util._

/**
  * 上下文标签主类
  * 商圈打标签
  */
object TagsContext_Buss_self {

  def main(args: Array[String]): Unit = {
/*    if(args.length!=1){
      println("目录不正确")
      sys.exit()
    }
    val Array(inputPath)=args*/

    // 创建Spark上下文
    val spark = SparkSession.builder().appName("Tags").master("local").getOrCreate()
    import spark.implicits._

    // 读取数据文件
    val df: DataFrame = spark.read.parquet("F:\\tmp\\outputall2")

    // 处理数据信息
   // df.map((_,1)).rdd


    val userId2BusinessTagRDD = df.map(row => {
      // 获取用户的唯一ID
      val userId = TagUtils.getOneUserId(row)
      // 商圈
      val businessList = BusmakeTags(row)

      (userId, businessList)
    }).rdd


    val userId2FilterTagRDD: RDD[(String, List[(String, Int)])] = userId2BusinessTagRDD.filter {
      //x=>x._2>0
      case (userId, listBusinessTag) => listBusinessTag.size > 0
    }

    val userTagRDD = userId2FilterTagRDD.map {
      case (userId, listBTag) =>
        var aggInfo: String = ""
        for (item <- listBTag) {
          val name = item._1
          val count = item._2
          aggInfo += name + "=" + count + "|"
        }

          aggInfo = aggInfo.substring(0, aggInfo.length - 1)

        aggInfo
    }
    userTagRDD.foreach(println)


  }

  def BusmakeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    // 获取数据
    val row = args(0).asInstanceOf[Row]
    // 获取经纬度
    if(String2Type.toDouble(row.getAs[String]("long")) >=73
      && String2Type.toDouble(row.getAs[String]("long")) <=136
      && String2Type.toDouble(row.getAs[String]("lat"))>=3
      && String2Type.toDouble(row.getAs[String]("lat"))<=53){
      // 经纬度
      val long = row.getAs[String]("long").toDouble
      val lat = row.getAs[String]("lat").toDouble
      // 获取到商圈名称
      val business = getBusiness(long,lat)
      if(StringUtils.isNoneBlank(business)){
        val str = business.split(",")
        str.foreach(str=>{
          //list:+=(str,1)
          list = list :+ (str,1)
        })
      }
    }
    list
  }

  def getBusiness(long:Double,lat:Double):String={
    // GeoHash码
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,6)
    // redis数据库查询当前商圈信息
    var business = redis_queryBusiness(geohash)
    // 不存在，，去高德请求
    if(business == null){
      business = getBusinessFromAmap(long,lat)
      // 将高德获取的商圈存储数据库
      if(business!=null && business.length>0){
        redis_insertBusiness(geohash,business)
      }
    }
    business
  }

  /**
    * 数据库获取商圈信息
    * @param geohash
    * @return
    */
  def redis_queryBusiness(geohash:String):String={
    val jedis = JedisConnectionPool.getConnection()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }
  /**
    * 将商圈保存数据库
    */
  def redis_insertBusiness(geohash:String,business:String): Unit ={
    val jedis = JedisConnectionPool.getConnection()
    jedis.set(geohash,business)
    jedis.close()
  }


  def getBusinessFromAmap(long:Double,lat:Double):String={
    // https://restapi.amap.com/v3/geocode/regeo?
    // location=116.310003,39.991957&key=59283c76b065e4ee401c2b8a4fde8f8b&extensions=all
    val location = long+","+lat
    // 获取URL
    val url = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=0b48ce502c74c9547b4616373e041fec"
    //调用Http接口发送请求,返回的是一个json串信息
    val jsonstr = HttpUtilget(url)
    //println(jsonstr)
    // 解析json串
    val jSONObject = JSON.parseObject(jsonstr)
    //println(jSONObject)
    // 判断当前状态是否为 1
    val status = jSONObject.getIntValue("status")
    if(status == 0) return ""
    // 如果不为空
    val regeocode = jSONObject.getJSONObject("regeocode")
    if(regeocode == null) return ""
    val addressComponent = regeocode.getJSONObject("addressComponent")
    if(addressComponent == null) return ""
    val businessAreas :JSONArray = addressComponent.getJSONArray("businessAreas")
    if(businessAreas == null) return  ""

   // val value: Array[AnyRef] = businessAreas.toArray()

    // 定义集合取值
    val result = collection.mutable.ListBuffer[String]()
    // 循环数组
    for (item <- businessAreas.toArray()){
      if(item.isInstanceOf[JSONObject]){
        //val nObject: JSONObject = item.asInstanceOf[JSONObject]
        //类型转换  引用，引用类型 转 JSON类型
        val nObject = item.asInstanceOf[JSONObject]
        val name = nObject.getString("name")
        result.append(name)
      }
    }
    // 商圈名字
    result.mkString(",")
  }

  val config = new JedisPoolConfig()

  config.setMaxTotal(20)

  config.setMaxIdle(10)

  private val pool = new JedisPool(config,"hadoop102",6379,10000,"123456")

  def getConnection():Jedis={
    pool.getResource
  }


  def HttpUtilget(url:String):String={

    val client = HttpClients.createDefault()

    val httpGet = new HttpGet(url)
    // 发送请求
    val httpResponse = client.execute(httpGet)
    // 处理返回请求结果
    EntityUtils.toString(httpResponse.getEntity,"UTF-8")
  }



}

//case class UserTag(userId:String, BTag:String )