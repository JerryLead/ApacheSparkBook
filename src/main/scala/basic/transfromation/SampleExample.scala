package basic.transfromation

import org.apache.spark.sql.SparkSession

object SampleExample {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder.appName("sample example").master("local[1]").getOrCreate()
    val sc = spark.sparkContext
    // val inputRDD = sc.parallelize(List(1,2,3,4,5,6,7,8), 3)
    val inputRDD = sc.parallelize(
      List((1,'c'),(1,'f'),(1,'a'),(1,'d'),(1,'d'),(2,'h'),(2,'h'),(2,'b'),(2,'e'),(2,'g')), 3)
//    val inputRDD = sc.parallelize(1 to 1000,1)

    println("---------------sample false 0.5")
    var sampleRDD = inputRDD.sample(false, 0.5)
//    sampleRDD.foreach(println)
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", InputRDD-value: " + value)
    }).foreach(println)
    println(sampleRDD.toDebugString)
    sampleRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    println("---------------sample true 2")
    sampleRDD = inputRDD.sample(true, 0.5)
//    sampleRDD.foreach(println)
    println(sampleRDD.toDebugString)
    sampleRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println(sampleRDD.toDebugString)

//    println("---------------take sample false 1")
//    var takeSampleRDD = inputRDD.takeSample(false,1)
//    println(sampleRDD.toDebugString)
//    sampleRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)
//    println(sampleRDD.toDebugString)
//
//    println("---------------take sample false 8")
//    takeSampleRDD = inputRDD.takeSample(false,8)
//    println(sampleRDD.toDebugString)
//    sampleRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)
//    println(sampleRDD.toDebugString)
//
//    println("---------------take sample true 1")
//    takeSampleRDD = inputRDD.takeSample(true,1)
//    println(sampleRDD.toDebugString)
//    sampleRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)
//    println(sampleRDD.toDebugString)
//
//    println("---------------take sample true 8")
//    takeSampleRDD = inputRDD.takeSample(true,8)
//    println(sampleRDD.toDebugString)
//    sampleRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)
//    println(sampleRDD.toDebugString)
  }
}
