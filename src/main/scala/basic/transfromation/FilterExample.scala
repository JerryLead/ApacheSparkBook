package basic.transfromation

import org.apache.spark.sql.SparkSession

object FilterExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("filter example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

//    val data = Array[(Int, Char)](
//      (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),
//      (3,'f'),(2,'g'),(1,'h')
//    )
//    val inputRDD = sc.parallelize(data, 3)
//
//    println("------------input rdd----------")
//    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)
//
//    val resultRDD = inputRDD.filter(obj => obj._1%2==0)
//    println(resultRDD.toDebugString)
//    println("------------result rdd----------")
//    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)


    val inputRDD = sc.parallelize(Array[(Int, Char)](
      (1,'a'),(2,'b'),(3,'c'),(4,'d'),(2,'e'),
      (3,'f'),(2,'g'),(1,'h')
    ), 3)

    println("------------input rdd----------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.filter(r => r._1 % 2 == 0)
    println(resultRDD.toDebugString)
    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
