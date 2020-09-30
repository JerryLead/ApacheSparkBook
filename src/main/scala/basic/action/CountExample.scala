package basic.action

import org.apache.spark.sql.SparkSession

object CountExample {
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
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val result1 = inputRDD.count()
    println(result1)

    val result2 = inputRDD.countByKey()
    println(result2)

    val result3 = inputRDD.countByValue()
    println(result3)

    // inputRDD.saveAsSequenceFile()

  }

}
