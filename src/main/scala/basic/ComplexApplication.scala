package basic

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object ComplexApplication {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Complex application")
      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext

    val data1 = Array[(Int, Char)](
      (1, 'a'), (2, 'b'),
      (3, 'c'), (4, 'd'),
      (5, 'e'), (3, 'f'),
      (2, 'g'), (1, 'h'))
    val rdd1 = sc.parallelize(data1, 3)
    val partitionedRDD = rdd1.partitionBy(new HashPartitioner(3))

//    println("----- rdd1-----")
//    rdd1.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)

    val data2 = Array[(Int, String)]((1, "A"), (2, "B"),
      (3, "C"), (4, "D"))
    val rdd2 = sc.parallelize(data2, 2).map(x => (x._1, x._2 + "" + x._2))

//    println("----- rdd2 -----")
//    rdd2.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)

    val data3 = Array[(Int, String)]((3, "X"), (5, "Y"), (3, "Z"), (4, "Y"))
    val rdd3 = sc.parallelize(data3, 2)


    val unionedRDD = rdd2.union(rdd3)


    val result = partitionedRDD.join(unionedRDD)

    result.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    println(result.toDebugString)

    System.in.read()
  }

}
