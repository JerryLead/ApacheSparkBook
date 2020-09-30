package basic.transfromation

import org.apache.spark.sql.SparkSession

object Coalesce1Example {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession
      .builder
      .appName("coalesce example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    //val inputRDD = sc.parallelize(1 to 10, 5)
    val inputRDD = sc.parallelize(Array[(Int,Char)](
      (3,'c'),(3,'f'),(1,'a'),(4,'d'),(1,'h'),(2,'b'),(5,'e'),(2,'g')
    ),5)

    println("------------input rdd----------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val coalesceRDD = inputRDD.coalesce(6, true)
    //val coalesceRDD = inputRDD.coalesce(7,true)
    println(coalesceRDD.toDebugString)
    println(coalesceRDD.partitions.length)
    println("------------result rdd----------")
    coalesceRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println(coalesceRDD.toDebugString)

  }
}
