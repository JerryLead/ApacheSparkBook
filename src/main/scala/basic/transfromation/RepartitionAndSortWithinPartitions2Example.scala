package basic.transfromation

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object RepartitionAndSortWithinPartitions2Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("patitionBy example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(Array[(Char,Int)](
      // (3,'c'),(3,'f'),(1,'a'),(4,'d'),(1,'h'),(2,'b'),(5,'e'),(2,'g')
      ('D',2), ('B',4), ('C',3), ('A',5), ('B',2), ('C',1), ('C',3), ('A',4)
    ),3)

    println("-------------input rdd------------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.repartitionAndSortWithinPartitions(new HashPartitioner(2))
    println(resultRDD.toDebugString)
    println("-------------result rdd------------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
