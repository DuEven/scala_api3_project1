import java.util

import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, Row, SparkSession}

object ApiDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[2]")
      .appName("API demo")
      .getOrCreate()

    val frame: DataFrame = sparkSession.read.parquet("person.parquet")
    //action类型的方法
    //展示前20行，默认字符串长度超过20，进行部分展示
//    frame.show()
//    frame.show(20)
//    frame.show(20,true)
//    frame.show(true)
    //展示前20行，自定义字符串需要截取的长度
    frame.show(20,2)
    //收集结果到集合中
    val rows: Array[Row] = frame.collect()
    val list: util.List[Row] = frame.collectAsList()
    println(rows.toBuffer)

    //frame.write.parquet("12.parquet")
    //返回某列的统计结果：count，mean，stddev,min,max
    val frame1: DataFrame = frame.describe("age")
    frame1.show()

    //
    frame.printSchema()
    //原生展示前N行
    //take(3)=>head(3)
//    frame.take(3)
//    frame.head(3)
//    // frame.first() head()=>head(1)
//    frame.first()
//    frame.takeAsList(3)
//
//    //transformation
//    //选取需要的列，返回一个新的DF,DS
//    val frame2: DataFrame = frame.select("name")
//    frame.select($"age")
//    frame.select(frame.col("name"))
//    frame.selectExpr("name","age+1","gender as gd")
//    frame.select(expr("name"),expr("age+1"),expr("gender as gd"))
//    //where.filter,对结果根据条件进行过滤，可以多个条件组合 and or
//    frame.select("age").where($"age">20)
//    frame.select("age").filter($"age">20)
//    frame.select("age","name").filter($"age">18 and $"age"<30 )
//    //frame.select("age","name").filter($"age">18 and $"name"=="aa" )
//    //多个条件，最后放到一个字符串中表示。
//    frame.select("age","name").filter("$'age'>18 and $'name'=='aa'")
//    frame.select("age","name").filter("$'age'>18 & $'name'=='aa'")
//    //一般分组之后，可以组内聚合
//    frame.groupBy("gender")
//    frame.groupBy(frame.col("gender"))
//    frame.groupBy(frame("gender"))
//    frame.groupBy(frame.apply("gender"))
//    val dataset: RelationalGroupedDataset = frame.groupBy("name","gender")
//    dataset.max("age")
//    //agg经常和groupBy一起使用，组内聚合
//    frame.agg(max("age"),min("age"))
//    frame.groupBy("gender").agg(max("age"),min("age"))
//    //排序
//    frame.orderBy("age")
//    frame.sort(frame.col("age"))
//    //增加一列
//    frame.withColumn("add",frame("name"))
//    //某一列列名重命名
//    frame.withColumnRenamed("gender","new_gender")
   //join，会shuffle
    // `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
  //                `right`, `right_outer`, `left_semi`, `left_anti`.
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    val frame3: DataFrame = frame.join(frame,"name")
    val ds: Dataset[Row] = frame3.limit(2)
    frame.show()
    ds.show()
    //返回的左边能右边数据集匹配上的所有元素形成的一个新的数据集
    //===，比较，返回结果元素true,false，column类型
   // val frame4: DataFrame = frame.join(ds,frame("name")===ds("name"),"left_semi")
    //返回的左边不能和右边数据集匹配上的所有元素形成的一个新的数据集
    val frame4: DataFrame = frame.join(ds,frame("name")===ds("name"),"left_anti")
    frame4.show()
    //查看执行计划
    frame3.explain()
    sparkSession.stop()
  }

}
