package basic.transfromation

import org.apache.spark.sql.SparkSession

object RepartitionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("reducebykey example").master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(1 to 10, 5)

    println("------------input rdd----------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.repartition(3)
    println(resultRDD.toDebugString)
    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
