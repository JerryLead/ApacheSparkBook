package basic.transfromation

import org.apache.spark.sql.SparkSession

object SortByKeyExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("sort by key example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    val inputRDD = sc.parallelize(Array[(Char,Int)](
      ('D',2), ('B',4), ('C',3), ('A',5), ('B',2), ('C',1), ('C',3), ('A',4)
    ),3)
    println("------------input rdd----------")
//    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)
    println("------------result rdd----------")
    val sortByKeyRDD = inputRDD.sortByKey(true,2)
    println(sortByKeyRDD.toDebugString)
    sortByKeyRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    System.in.read()
  }
}
