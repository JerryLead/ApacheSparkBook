package basic.transfromation

import org.apache.spark.sql.SparkSession

object ZipWithIndexOrUniqueIdExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("zip with index or unique example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

//    val inputRDD = sc.parallelize(1 to 9,3)

    var inputRDD = sc.parallelize(Array[(Int, Char)](
      (1,'a'),(1,'b'),(2,'c'),(3,'d'),(4,'e'),(5,'f'),(6,'g')
    ),3)

    println("-------------input rdd------------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    var resultRDD = inputRDD.zipWithIndex()
    println(resultRDD.toDebugString)
    println("------------zip with index result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    resultRDD = inputRDD.zipWithUniqueId()
    println(resultRDD.toDebugString)
    println("------------zip with uniqueid result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
