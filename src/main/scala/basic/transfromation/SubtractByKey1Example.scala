package basic.transfromation

import org.apache.spark.{HashPartitioner, RangePartitioner}
import org.apache.spark.sql.SparkSession

object SubtractByKey1Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("subtract by key example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

    var inputRDD1 = sc.parallelize(Array[(Int,Char)](
      (3,'c'),(3,'f'),(5,'e'),(4,'d'),(1,'h'),(2,'b'),(5,'e'),(2,'g')
    ),3)
    //inputRDD1 = inputRDD1.repartitionAndSortWithinPartitions(new HashPartitioner(3))
    var inputRDD2 = sc.parallelize(Array[(Int,Char)](
      (1,'A'),(2,'B'),(3,'C'),(2,'D'),(6,'E')
    ),2)

    println("-------------input rdd1------------")
    inputRDD1.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println("-------------input rdd2------------")
    inputRDD2.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    println("------------result rdd----------")
    val subtractByKeyRDD = inputRDD1.subtractByKey(inputRDD2,2)
    println(subtractByKeyRDD.toDebugString)
    subtractByKeyRDD.mapPartitionsWithIndex((pid, iter)=>{
      if (iter.hasNext){
        iter.map( value => "PID: " + pid + ", value: " + value)
      }
      else{
        List("PID: " + pid + ", value: NULL").iterator
      }
    }).foreach(println)
  }
}
