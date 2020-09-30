package basic.transfromation

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object Join1Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("join1 example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

    var inputRDD1 = sc.parallelize(Array[(Int,Char)](
      //(3,'c'),(3,'f'),(1,'a'),(4,'d'),(1,'h'),(2,'b'),(5,'e'),(2,'g')
      (1,'a'),(1,'b'),(2,'c'),(3,'d'),(4,'e'),(5,'f')
    ),3)
    inputRDD1 = inputRDD1.repartitionAndSortWithinPartitions(new HashPartitioner(3))

    var inputRDD2 = sc.parallelize(Array[(Int,Char)](
      //(2,'A'),(1,'B'),(3,'C'),(4,'D'),(6,'E'), (2,'F'), (2,'G')
      (1,'A'),(3,'B'),(2,'C'),(2,'D'),(2,'E')
    ),3)
    inputRDD2 = inputRDD2.repartitionAndSortWithinPartitions(new HashPartitioner(3))

    println("-------------input rdd1------------")
    inputRDD1.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "rdd1-PID: " + pid + ", value: " + value)
    }).foreach(println)
    println("-------------input rdd2------------")
    inputRDD2.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "rdd2-PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD1.join(inputRDD2,3)
    println(resultRDD.toDebugString)
    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
