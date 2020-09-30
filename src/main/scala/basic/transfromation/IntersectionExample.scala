package basic.transfromation

import org.apache.spark.sql.SparkSession

object IntersectionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("itersection example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
//    val inputRDD1 = sc.parallelize(List(1,2,3,3,4,5), 3)
//    val inputRDD2 = sc.parallelize(List(1,2,5,6), 2)

    val inputRDD1 = sc.parallelize(List(2,2,3,4,5,6,8,6),3)
    val inputRDD2 = sc.parallelize(List(2,3,6,6),2)


    //    println("-------------input rdd1------------")
    inputRDD1.mapPartitionsWithIndex((pid, iter)=>{
      if (iter.hasNext){
        iter.map(value => "PID: " + pid + ", value: " + value)
      }else{
        List("PID: " + pid + ", value: NULL").iterator
      }
    }).foreach(println)
    println("-------------input rdd2------------")
    inputRDD2.mapPartitionsWithIndex((pid, iter)=>{
      if (iter.hasNext){
        iter.map(value => "PID: " + pid + ", value: " + value)
      }else{
        List("PID: " + pid + ", value: NULL").iterator
      }
//      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD1.intersection(inputRDD2,2)
    println(resultRDD.toDebugString)
    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      if (iter.hasNext){
        iter.map(value => "PID: " + pid + ", value: " + value)
      }else{
        List("PID: " + pid + ", value: NULL").iterator
      }
//      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
