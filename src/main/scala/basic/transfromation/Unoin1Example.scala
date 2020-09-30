package basic.transfromation

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object Unoin1Example {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession
      .builder
      .appName("union example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

//    var inputRDD1 = sc.parallelize(List(2,2,3,4,5,6,8,6),3)
//    inputRDD1 = inputRDD1.repartition(3)
//    var inputRDD2 = sc.parallelize(List(2,3,6,6),2)
//    inputRDD2 = inputRDD2.repartition(3)
    var inputRDD1 = sc.parallelize(Array[(Int,Char)](
      (1,'a'), (2,'b'), (3,'c'), (4, 'd'), (5,'e'), (3,'f'), (2,'g'), (1,'h'), (2,'i')
    ),3)
    var inputRDD2 = sc.parallelize(Array[(Int,Char)](
      (1,'A'),(2,'B'),(3,'C'),(4,'D'),(6,'E')
    ),2)
    inputRDD1 = inputRDD1.repartitionAndSortWithinPartitions(new HashPartitioner(3))
    inputRDD2 = inputRDD2.repartitionAndSortWithinPartitions(new HashPartitioner(3))

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

    System.in.read()

  }
}
