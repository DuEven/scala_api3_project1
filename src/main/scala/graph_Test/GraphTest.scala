package graph_Test

import org.apache.spark.graphx.Edge
import org.apache.spark.sql.SparkSession

/**
  * @author Duyw
  *         Date:2019/09/23  10:36:26
  * @Version ：1.0
  * @description:
  *
  */
object GraphTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("garph").master("local[*]").getOrCreate()

    //创建点和边
    //创建点的集合
    spark.sparkContext.makeRDD(Seq(
      (1L,("小鼠",21)),
      (2L,("小牛",22)),
      (3L,("小虎",23)),
      (4L,("小兔",24)),
      (5L,("小龙",25)),
      (6L,("小蛇",26)),
      (7L,("小马",27)),
      (8L,("小羊",28)),
      (9L,("小猴",29)),
      (10L,("小鸡",30)),
      (11L,("小狗",11)),
      (12L,("小猪",11)),
    ))
    //构造边的集合
    spark.sparkContext.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(16L, 133L, 0),
      Edge(21L, 133L, 0),
      Edge(44L, 133L, 0),
      Edge(1L, 133L, 0)
    ))

    //构件图

    Graph()


  }

}
