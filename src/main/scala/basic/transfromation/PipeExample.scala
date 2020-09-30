package basic.transfromation

import org.apache.spark.sql.SparkSession

object PipeExample {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder.appName("pipe example").master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.parallelize(List(1,2,4,5,6), 2)

    val pipeRDD = rdd.pipe("head -n 2")
    println(pipeRDD.toDebugString)

    pipeRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println(pipeRDD.toDebugString)
  }
}
