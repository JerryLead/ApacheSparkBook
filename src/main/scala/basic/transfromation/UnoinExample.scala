package basic.transfromation

import org.apache.spark.sql.SparkSession

object UnoinExample {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession
      .builder
      .appName("union example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputRDD1 = sc.parallelize(List(2,2,3,4,5,6,8,6),3)
    val inputRDD2 = sc.parallelize(List(2,3,6,6),2)

    println("-----------input rdd1--------")
    inputRDD1.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println("-----------input rdd2--------")
    inputRDD2.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD1.union(inputRDD2)
    println(resultRDD.toDebugString)
    println("-----------result rdd--------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println(resultRDD.toDebugString)
    println(resultRDD.collect.toList)
    println(resultRDD.toDebugString)

  }
}
