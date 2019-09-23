package Tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import util.Tag

/**
  * 广告标签
  */
object TagsApp {

  def makeTags(arg0:Row,arg1:Broadcast[collection.Map[String, String]]): List[(String, Int)] = {

    var list =List[(String,Int)]()
    // 获取数据类型
    val row = arg0.asInstanceOf[Row]
    // 获取广告位类型和名称
    var appType = row.getAs[String]("appname")
    if(StringUtils.isBlank(appType)){   //如果是空，采用appid来映射
      appType = arg1.value.getOrElse(row.getAs[String]("appid"),"unknow")
    }
    // 广告位类型标签
    appType match {
      //case v => list:+=("LC"+v,1)
      case v => list :+= ("APP"+v,1)
    }
    list

  }
}
