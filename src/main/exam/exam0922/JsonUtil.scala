package exam0922

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
  * @author Duyw
  *         Date:2019/09/22  10:54:19
  * @Version ：1.0
  * @description:
  *
  */
object JsonUtil {
  def getBusiness(line:String,str:String):String={
    val x: String = line.toString()
    val jSONObject = JSON.parseObject(x)

    val status = jSONObject.getIntValue("status")

    if (status == 0) return ""

    val regeocode = jSONObject.getJSONObject("regeocode")
    if (regeocode == null) return ""

    val poisObj = regeocode.getJSONArray("pois")
    if (poisObj == null) return ""

    // 定义集合取值
    var result = collection.mutable.ListBuffer[String]()

    // 循环数组
    for (item <- poisObj.toArray()) {
      if (item.isInstanceOf[JSONObject]) {
        //val nObject: JSONObject = item.asInstanceOf[JSONObject]
        //类型转换  引用，引用类型 转 JSON类型
        val nObject = item.asInstanceOf[JSONObject]
        val str1 = nObject.getString(str)
        result.append(str1)
      }
    }
    result.mkString(",")
  }


}
