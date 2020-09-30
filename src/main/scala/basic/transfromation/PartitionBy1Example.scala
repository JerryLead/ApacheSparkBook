package basic.transfromation

import org.apache.spark.{Partitioner, RangePartitioner}
import org.apache.spark.sql.SparkSession

object PartitionBy1Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("repatition and sort with index example")
      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(Array[(Int, Char)](
      (1,'a'),(2,'b'),(3,'c'),(4,'d'),(2,'e'),
      (3,'f'),(2,'g'),(1,'h')
    ), 3)

    println("------------input rdd----------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.partitionBy(new RangePartitioner(2,inputRDD))
    println(resultRDD.toDebugString)
    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
