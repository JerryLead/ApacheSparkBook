package basic.transfromation

import org.apache.spark.sql.SparkSession

object SubtractExample {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession
      .builder
      .appName("subtract example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputRDD1 = sc.makeRDD(List(1,2,3,3,4,2,1,5,6),3)
//    val input2 = sc.makeRDD(List(7,8,7,9,10),2)
    val inputRDD2 = sc.makeRDD(List(1,1,3,4,10),2)

    println("-----------input rdd1--------")
    inputRDD1.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println("-----------input rdd2--------")
    inputRDD2.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD1.subtract(inputRDD2,2)
    println(resultRDD.toDebugString)
    println("-----------result--------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
