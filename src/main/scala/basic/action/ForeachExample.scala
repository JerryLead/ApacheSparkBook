package basic.action

import org.apache.spark.sql.SparkSession

object ForeachExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("filter example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(Array[(Int,Char)](
      (3,'c'),(3,'f'),(5,'e'),(4,'d'),(1,'h'),(2,'b'),(5,'e'),(2,'g')
    ),3)

    println("------------input rdd----------")
    inputRDD.foreach(r => println(r._2))

    inputRDD.foreachPartition(iter => {
      while(iter.hasNext) {
        val record = iter.next()
        if (record._1 >= 3)
          println(record)
      }
    })

  }
}
