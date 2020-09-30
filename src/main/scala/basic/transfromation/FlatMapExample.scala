package basic.transfromation

import org.apache.spark.sql.SparkSession

object FlatMapExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder.
      appName("filter map example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

//    val data = List("ubuntu linux", "windows10", "centos linux")
//    val inputRDD = sc.parallelize(data, 3)
    val inputRDD = sc.parallelize(Array[String](
      "how do you do","are you ok","thanks","bye bye",
      "I'm ok"), 3)


    println("------------input rdd----------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.flatMap(x=>x.split(" "))
    println(resultRDD.toDebugString)

    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
