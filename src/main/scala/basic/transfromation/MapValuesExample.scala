package basic.transfromation

import org.apache.spark.sql.SparkSession

object MapValuesExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("map values example")
      .master("local[1]")
      .getOrCreate()

    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(Array[(Int, Char)](
      (1,'a'),(2,'b'),(3,'c'),(4,'d'),(2,'e'),
      (3,'f'),(2,'g'),(1,'h')
    ), 3)

    println("-----------input RDD--------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.mapValues(x=>x+"_1")
    println(resultRDD.toDebugString)
    println("-----------result RDD--------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
