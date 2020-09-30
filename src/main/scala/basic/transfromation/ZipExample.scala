package basic.transfromation

import org.apache.spark.sql.SparkSession

object ZipExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("zip example").master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    val inputRDD1 = sc.parallelize(1 to 8,3)
    val inputRDD2 = sc.parallelize('a' to 'h',3)

    println("-------------input rdd1------------")
    inputRDD1.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println("-------------input rdd2------------")
    inputRDD2.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD1.zip(inputRDD2)
    println(resultRDD.toDebugString)
    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
