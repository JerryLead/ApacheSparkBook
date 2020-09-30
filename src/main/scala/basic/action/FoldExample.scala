package basic.action

import org.apache.spark.sql.SparkSession

object FoldExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("reduce by key example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

//    var inputRDD = sc.parallelize(Array[(Int,String)](
//      (1,"a"),(2,"b"),(3,"c"),(4,"d"),(2,"e"),(3,"f"),(2,"g"),(1,"h"),(2,"i")
//    ),3)

    var inputRDD = sc.parallelize(Array[(String)](
      "a", "b", "c", "d", "e", "f", "g", "h", "i"),4)

    var inputRDD2 = sc.parallelize(Array[(Int)](
      1, 2, 3, 4, 5, 6, 7, 8, 9),3)

    println("------------input rdd----------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val result = inputRDD.fold("0")((x,y) => x + "_" + y)
    println(result)

    val result2 = inputRDD.reduce((x,y) => x + "_" + y)
    println(result2)

    val result3 = inputRDD.aggregate("0")((x,y) => x + "_" + y, (x,y) => x + "@" + y)
    println(result3)

    val result4 = inputRDD.treeAggregate("0")((x,y) => x + "_" + y, (x,y) => x + "@" + y)
    println(result4)

    System.in.read()

  }
}
