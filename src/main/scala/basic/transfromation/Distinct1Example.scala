package basic.transfromation

import org.apache.spark.sql.SparkSession

object Distinct1Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("group by key example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(Array[(Int,Char)](
      (2,'b'),(3,'f'),(1,'a'),(4,'d'),(5,'e'),(3,'f'),(2,'b'),(1,'h'),(2,'i')
    ),3)

    println("------------input rdd----------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.distinct(2)
    println(resultRDD.toDebugString)
    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
