package basic.transfromation

import org.apache.spark.sql.SparkSession

object FlatMapValuesExample1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("flat map values example")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(Array[(Int, String)](
      (1,"how do you do"),(2,"are you ok"),(4,"thanks"),(5,"bye bye"),
      (2,"I'm ok")
    ), 3)
    println("-----------input RDD--------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    //    val resultRDD = inputRDD.flatMapValues(x=>x+"_")
    val resultRDD = inputRDD.flatMapValues(x=>x.split(" "))
    println(resultRDD.toDebugString)
    println("-----------result RDD--------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

  }
}
