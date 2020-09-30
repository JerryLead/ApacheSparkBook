package basic.transfromation

import org.apache.spark.sql.SparkSession

object MapPatitionsInternalExample {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession
      .builder
      .appName("map patition with index example")
      .master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    val inputRDD = sc.parallelize(List(1,2,4,5,6,2,4), 2)

    println("-------------input rdd------------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.mapPartitions(iter => {
      var result = List[Int]()
      var i = 0
      while(iter.hasNext){
        i += iter.next

      }
      result.::(i).iterator
    }, preservesPartitioning = true)
    println(resultRDD.toDebugString)
    println("-------------result rdd------------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
