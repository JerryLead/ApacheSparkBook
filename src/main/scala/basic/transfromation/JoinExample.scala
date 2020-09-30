package basic.transfromation

import org.apache.spark.sql.SparkSession

object JoinExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("join example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    val inputRDD = sc.parallelize(Array[(Int,Char)](
      (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(3,'f'),(2,'g'),(1,'h')
    ),3)
    val rdd2 = sc.parallelize(Array[(Int,Char)](
      (1,'A'),(2,'B'),(3,'C'),(4,'D'),(6,'E')
    ),2)
//    println("-------------input rdd1------------")
//    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)
//    println("-------------input rdd2------------")
//    rdd2.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)
    val resultRDD = inputRDD.join(rdd2,3)
    println(resultRDD.toDebugString)
    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println(resultRDD.toDebugString)

  }
}
