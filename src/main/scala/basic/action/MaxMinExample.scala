package basic.action

import org.apache.spark.sql.SparkSession

object MaxMinExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Max-min example")
      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext


    val inputRDD = sc.parallelize(Array[Int](
      3, 2, 4, 4, 1, 5, 2, 5, 2
    ), 3)

    println("------------input rdd----------")
    inputRDD.mapPartitionsWithIndex((pid, iter) => {
      iter.map(value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val result1 = inputRDD.max()
    println(result1)

    val result2 = inputRDD.min()
    println(result2)

  }
}
