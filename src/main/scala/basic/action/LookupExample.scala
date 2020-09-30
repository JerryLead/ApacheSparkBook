package basic.action

import org.apache.spark.sql.SparkSession

object LookupExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("lookup example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(Array[(Int, Char)](
      (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (2, 'e'), (3, 'f'), (2, 'g'), (1, 'h')
    ), 3)

    println("------------input rdd----------")
    val result = inputRDD.lookup(2)

    println(result)

  }


}
