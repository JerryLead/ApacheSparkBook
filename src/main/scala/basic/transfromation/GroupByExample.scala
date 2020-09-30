package basic.transfromation

import org.apache.spark.sql.SparkSession

object GroupByExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("zip with index or unique example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(1 to 9,3)

    println("-------------input rdd------------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    var resultRDD = inputRDD.groupBy(x => x%2)
    println(resultRDD.toDebugString)
    println("------------zip with index result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
