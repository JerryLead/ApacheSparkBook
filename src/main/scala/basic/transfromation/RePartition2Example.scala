package basic.transfromation

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object RePartition2Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("patitionBy example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(Array[(Int,Char)](
      (3,'c'),(3,'f'),(1,'a'),(4,'d'),(1,'h'),(2,'b'),(5,'e'),(2,'g')
    ),3)

    println("-------------input rdd------------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.repartition(2)
    println(resultRDD.toDebugString)
    println("-------------result rdd------------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
