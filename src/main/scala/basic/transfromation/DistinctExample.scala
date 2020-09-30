package basic.transfromation

import org.apache.spark.sql.SparkSession

object DistinctExample {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession
      .builder
      .appName("distinct example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(List(1,2,2,3,2,1,4,3,4), 3)

    println("------------input rdd----------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.distinct(2)
    println(resultRDD.toDebugString)
    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
