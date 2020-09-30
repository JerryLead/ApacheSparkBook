package basic.transfromation

import org.apache.spark.sql.SparkSession

object CoalesceExample {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession
      .builder
      .appName("coalesce example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    val inputRDD = sc.parallelize(1 to 10, 5)

//    println("------------input rdd----------")
//    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)

    val resultRDD = inputRDD.coalesce(7,false)
    println(resultRDD.toDebugString)
    println(resultRDD.partitions.length)
    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
