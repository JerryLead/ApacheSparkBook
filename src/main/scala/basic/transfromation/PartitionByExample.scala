package basic.transfromation

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object PartitionByExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("patitionBy example")
      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(Array[(Int, Char)](
      (1,'a'),(2,'b'),(3,'c'),(4,'d'),(2,'e'),
      (3,'f'),(2,'g'),(1,'h')
    ), 3)


    println("-------------input rdd------------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.partitionBy(new HashPartitioner(2))
    println(resultRDD.toDebugString)
    println("-------------result rdd------------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
