package basic.transfromation

import org.apache.spark.sql.SparkSession

object AggregateByKey1Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("reduce by key example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

//    val inputRDD = sc.parallelize(Array[(Char,Int)](
//      ('A',2),('B',1),('C',1),('B',1),('C',1),('D',1),('C',1),('A',1)
//    ),3)
//    var inputRDD = sc.parallelize(Array[(Int,Char)](
//      (1,'a'),(2,'b'),(2,'c'),(4,'d'),(5,'e'),(4,'f'),(2,'g'),(1,'h'),(2,'i')
//    ),3)
//    var inputRDD = sc.parallelize(Array[(Int,Char)](
//      (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(3,'f'),(2,'g'),(1,'h'),(2,'i')
//    ),3)
    var inputRDD = sc.parallelize(Array[(Int,String)](
      (1,"a"),(2,"b"),(3,"c"),(4,"d"),(2,"e"),(3,"f"),(2,"g"),(1,"h"),(2,"i")
    ),3)
//    inputRDD.repartitionAndSortWithinPartitions(new HashPartitioner(2))

    println("------------input rdd----------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

//    val resultRDD = inputRDD.reduceByKey((x,y) => x+y,2)
    val resultRDD = inputRDD.aggregateByKey("x",2)(_+"_"+_,_+"@"+_)
    println(resultRDD.toDebugString)
    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
