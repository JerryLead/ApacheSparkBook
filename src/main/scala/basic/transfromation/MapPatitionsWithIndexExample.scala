package basic.transfromation

import org.apache.spark.sql.SparkSession

object MapPatitionsWithIndexExample {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession
      .builder
      .appName("map patition with index example")
      .master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    val inputRDD = sc.parallelize(List(1,2,3,4,5,6,7,8,9), 3)

    println("-------------input rdd------------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.mapPartitionsWithIndex((pid, iter) => {
      var result = List[String]()
//      var i = 0
//      while(iter.hasNext){
//        i += iter.next
//      }

      var odd = 0
      var even = 0

      while (iter.hasNext) {
        val value = iter.next()
        if (value % 2 == 0)
          even += value
        else
          odd += value
      }
      result = result :+ "pid = " + pid + ", odd = " + odd
      result = result :+ "pid = " + pid + ", even = " + even
      result.iterator
    })
    println(resultRDD.toDebugString)
    println("-------------result rdd------------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
