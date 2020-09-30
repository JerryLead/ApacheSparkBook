package cache

import org.apache.spark.sql.SparkSession

object CacheTest2 {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Cache Test")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    var inputRDD = sc.parallelize(Array[(Int, String)](
      (1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (3, "f"), (2, "g"), (1, "h"), (2, "i")
    ), 3)

    val mappedRDD = inputRDD.map(r => (r._1 + 1, r._2))
    mappedRDD.cache()

    val reducedByKeyRDD = mappedRDD.reduceByKey((x, y) => x + "_" + y, 2)

    reducedByKeyRDD.cache()
    println("------------reducedByKeyRDD----------")
    reducedByKeyRDD.foreach(println)


    val groupedByKeyRDD = mappedRDD.groupByKey().mapValues(v => v.toList)

    groupedByKeyRDD.cache()
    println("------------groupedByKeyRDD----------")
    groupedByKeyRDD.foreach(println)
    // mappedRDD.unpersist()

    println(reducedByKeyRDD.toDebugString)
    println(groupedByKeyRDD.toDebugString)


    val joinedRDD = reducedByKeyRDD.join(groupedByKeyRDD)
    joinedRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    println("Join RDD")
    println(joinedRDD.toDebugString)
    System.in.read()

    spark.stop()
  }
}
