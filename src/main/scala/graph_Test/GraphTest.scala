package graph_Test

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author Duyw
  *         Date:2019/09/23  10:36:26
  * @Version ：1.0
  * @description: 图计算案例（好友关联推荐）
  *
  */
object GraphTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("garph").master("local[*]").getOrCreate()

    //创建点和边
    //创建点的集合
    val vertexRDD: RDD[(Long, (String, Int))] = spark.sparkContext.makeRDD(Seq(
      (1L, ("小1", 26)),
      (2L, ("小2", 30)),
      (6L, ("小6", 33)),
      (9L, ("小9", 26)),
      (133L, ("小133", 30)),
      (138L, ("小138", 33)),
      (158L, ("小158", 26)),
      (16L, ("小16", 30)),
      (44L, ("小44", 33)),
      (21L, ("小21", 26)),
      (5L, ("小5", 30)),
      (7L, ("小7", 33))
    ))
    //构造边的集合
    val edgeRDD: RDD[Edge[Int]] = spark.sparkContext.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      //dEdge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))

    //构件图
    val graph = Graph(vertexRDD,edgeRDD)
    //取顶点  那个点最小那个就是定点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    //匹配数据
    vertices.join(vertexRDD).map{
      case (userId,(cnId,(name,age))) => (cnId,List((name,age)))
    }
      .reduceByKey(_++_)
      .foreach(println(_))
  }

}
