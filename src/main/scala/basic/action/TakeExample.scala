package basic.action

import org.apache.spark.sql.SparkSession

object TakeExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("take example")
      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(Array[(Int, Char)](
      (3, 'c'), (3, 'f'), (5, 'e'), (4, 'd'), (1, 'h'), (2, 'b'), (5, 'e'), (2, 'g')
    ), 3)

    val inputRDD2 = sc.parallelize(Array[Int](
      3, 2, 4, 4, 1, 5, 2, 5, 2
    ), 3)

    println("------------input rdd----------")
    inputRDD.mapPartitionsWithIndex((pid, iter) => {
      iter.map(value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val result1 = inputRDD.take(3)
    println(result1.toList)

    val result2 = inputRDD.first()
    println(result2)

    val result3 = inputRDD2.takeOrdered(2)
    println(result3.toList)

    val result4 = inputRDD2.top(2)
    println(result4.toList)


  }

}
