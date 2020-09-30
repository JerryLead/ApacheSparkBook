package basic.transfromation

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object PartitionBy2Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("repatition and sort with index example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(Array[(Char,Int)](
      ('A',5),('B',4),('C',3),('B',2),('C',1),('D',2),('C',3),('A',4)
    ),3)

    println("------------input rdd----------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.partitionBy(new HashPartitioner(3))
    println(resultRDD.toDebugString)
    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
