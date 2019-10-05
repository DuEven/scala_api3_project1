package exam0928

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author 杜逸文
  *         Date:2019/09/28  12:02:40
  * @Version ：1.0
  * @description:
  *
  */
object test2 {
/*

  spark  思路：
  根据用户的行为时间进行 标签处理

  一、根据ods_user_action_log给用户打标签（user,(action,x),(action_targte,y),(ct,z)）

  可根据一下指标进行用户等级划分：
  1、行为  不同行为对应不同的积分  			action => (action,x)
  2、目标对象：购买不同的商品，给不同的积分   action_targte  => (action_targte,y)
  3、连续访问天数，给不同积分               	ct =>(ct,z)

  1、
  注释：x,y,z 分别对应积分

  二、聚合处理

  用户    总积分
  （user ,(x+y+z)）

  三、划分等级


  根据总积分划分等级
*/

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("aa").setMaster("local")

    val sc = new SparkContext(conf)

    val arr1 = ('A' to 'Z').toArray
    val arr2 = (1 to 26 ).toArray

    val rdd1: RDD[Char] = sc.makeRDD(arr1)
    val rdd2: RDD[Int] = sc.makeRDD(arr2)

    val rdd3: RDD[(Int, Char)] = rdd1.map(char => (char.toInt-64 ,char))
    val rdd4: RDD[(Int, Int)] = rdd2.map(n =>(n,n))

    val rdd5: RDD[(Int, (Char, Int))] = rdd3.join(rdd4)

    println(rdd1.getNumPartitions)
    println(rdd3.getNumPartitions)
    println(rdd1.partitioner)
    println(rdd3.partitioner)
    println(rdd5.partitioner)



   // rdd5.foreach(println)






  }
}
