package basic.transfromation

import org.apache.spark.sql.SparkSession

object RDDMapExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("map example")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(Array[(Int, Map[Int, Char])](
      (1, Map(1 -> 'a')),(2, Map(3 -> 'b')),(3, Map(6 -> 'c')), (4, Map(2 ->
        'd')),(2, Map(3 -> 'e')),
      (3, Map(2 -> 'f')),(2, Map(4 -> 'g')),(1, Map(5 -> 'h'))
    ), 3)

    val inputRDD2 = sc.parallelize(Array[Map[Int, Char]](
       Map(1 -> 'a'), Map(3 -> 'b'), Map(6 -> 'c'), Map(2 ->
        'd'), Map(3 -> 'e'),
      Map(2 -> 'f'), Map(4 -> 'g'), Map(5 -> 'h')
    ), 3)



    // val resultRDD = inputRDD.map(r => r._1 + "_" + r._2)
    val resultRDD = inputRDD2.map(r => r.keys)
    println(resultRDD.toDebugString)
    println("-----------result RDD--------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
