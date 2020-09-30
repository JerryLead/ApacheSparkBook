package basic.transfromation

import org.apache.spark.sql.SparkSession

object FoldByKeyExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("fold by key example")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

//    val inputRDD = sc.parallelize(Array[(Int, String)](
//      (1,"a"),(2,"b"),(3,"c"),(4,"d"),(1,"e"),
//      (3,"f"),(2,"g"),(1,"h")
//    ), 3)
    var inputRDD = sc.makeRDD(Array(("A",0),("A",2),("B",1),("C",1),("B",2),("D",1)),3)

    println("-----------input RDD--------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

//    val resultRDD = inputRDD.foldByKey("_")(_+_)
    val resultRDD = inputRDD.foldByKey(1)(_+_)
    println(resultRDD.toDebugString)
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
