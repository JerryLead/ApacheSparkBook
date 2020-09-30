package basic.transfromation

import org.apache.spark.sql.SparkSession

object MapExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("map example")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(Array[(Int, Char)](
      (1,'a'),(2,'b'),(3,'c'),(4,'d'),(2,'e'),
      (3,'f'),(2,'g'),(1,'h')
    ), 3)
//
//    println("-------------input rdd------------")
//    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)
//
////    val resultRDD = inputRDD.map(obj=>("key:"+obj._1, "val:"+obj._2))
//    val resultRDD = inputRDD.map(obj=>"key:"+obj._1)
//
//    println(resultRDD.toDebugString)
//    println("-------------result rdd------------")
//    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)
//
//    val data = List("ubuntu linux", "windows10", "centos linux")
//    val inputRDD = sc.parallelize(data)

    println("-------------input rdd------------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

//    val resultRDD = inputRDD.map(obj=>obj.split(" "))
//    val resultRDD = inputRDD.map(_.split(" "))
    val resultRDD = inputRDD.map(r => r._1 + "_" + r._2)
    println(resultRDD.toDebugString)
    println("-----------result RDD--------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }

}
