package basic.transfromation

import org.apache.spark.sql.SparkSession

object SampleByKeyExample {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder.appName("sample by key example").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    val inputRDD = sc.parallelize(
      List((1,'c'),(1,'f'),(1,'a'),(1,'d'),(1,'d'),(2,'h'),(2,'h'),(2,'b'),(2,'e'),(2,'g')), 3)
//    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)

    val map = Map((1->0.8),(2->0.5))
    println("---------------sample by key false")
    var sampleRDD = inputRDD.sampleByKey(false, map)
    println(sampleRDD.toDebugString)
    sampleRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    println("---------------sample by key true")
    sampleRDD = inputRDD.sampleByKey(true, map)
    println(sampleRDD.toDebugString)
//    sampleRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)

//    println("---------------sample by key exact false")
//    var takeSampleRDD = inputRDD.sampleByKeyExact(false,map)
//    println(sampleRDD.toDebugString)
//    sampleRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)
//
//    println("---------------sample by key exact true")
//    takeSampleRDD = inputRDD.sampleByKeyExact(true,map)
//    println(sampleRDD.toDebugString)
//    sampleRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)
  }
}
