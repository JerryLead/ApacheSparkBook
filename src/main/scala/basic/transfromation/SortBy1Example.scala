package basic.transfromation

import org.apache.spark.sql.SparkSession

object SortBy1Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("sort by example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    val inputRDD = sc.parallelize(List(1,2,4,5,6,2,4), 3)
    println("------------input rdd----------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println("------------result rdd----------")
    val sortByKeyRDD = inputRDD.sortBy(x=>x, false)
    println(sortByKeyRDD.toDebugString)
    sortByKeyRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
