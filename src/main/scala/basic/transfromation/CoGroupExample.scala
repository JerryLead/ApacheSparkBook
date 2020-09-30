package basic.transfromation

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object CoGroupExample {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession
      .builder
      .appName("cogroup example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

    var inputRDD1 = sc.parallelize(Array[(Int, Char)](
      (1,'a'),(1,'b'),(2,'c'),(3,'d'),(4,'e'),(5,'f')
    ),3)
//    inputRDD1 = inputRDD1.repartitionAndSortWithinPartitions(new HashPartitioner(3))
    val inputRDD2 = sc.parallelize(Array[(Int, Char)](
      (1,'f'),(3,'g'),(6,'h')
    ),2)

//    println("-------------input rdd1------------")
//    inputRDD1.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)
//    println("-------------input rdd2------------")
//    inputRDD2.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)

    val resultRDD = inputRDD1.cogroup(inputRDD2,3)
    println(resultRDD.toDebugString)
    println("------------result rdd----------")
//    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)
//    val groupWithRDD = inputRDD1.groupWith(inputRDD2)
//    println(groupWithRDD.toDebugString)
//    println("------------group with result rdd----------")
//    groupWithRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)
  }
}
