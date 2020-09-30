package basic.transfromation

import org.apache.spark.sql.SparkSession

object RandomSplitExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("random split example").master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(1 to 9,3)
    println("-------------input rdd------------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.randomSplit(Array(1.0,2.0))
    println(resultRDD.size)
    println("------------result rdd----------")
    var index = 0
    for(item <- resultRDD){
      println(item.toDebugString)
      println("------------result rdd "+index+"----------")
      item.mapPartitionsWithIndex((pid, iter)=>{
        iter.map( value => "PID: " + pid + ", value: " + value)
      }).foreach(println)
      index += 1
      println("------------------------------------------")
    }
  }
}
