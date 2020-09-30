package basic.transfromation

import org.apache.spark.sql.SparkSession

object FlatMapValuesExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("flat map values example")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

//    val inputRDD = sc.parallelize(Array[(Int, Char)](
//      (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),
//      (3,'f'),(2,'g'),(1,'h')
//    ), 3)
    val inputRDD = sc.parallelize(Array[(Int, String)](
      (1,"how do you do"),(2,"are you ok"),(3,"I am fine"),(4,"thanks"),(5,"byebye"),
      (3,"can I help you"),(2,"I'm ok"),(1,"hello")
    ), 3)
    println("-----------input RDD--------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

//    val resultRDD = inputRDD.flatMapValues(x=>x+"_")
    val resultRDD = inputRDD.flatMapValues(x=>x.split(" "))
    println(resultRDD.toDebugString)
    println("-----------result RDD--------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println(resultRDD.collect.toList)

    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val b = a.map(x => (x.length, x))
    println(b.collect().toList)
    println(b.flatMapValues(x=>"x" + x + "x").collect.toList)
    println(b.flatMapValues("x" + _ + "x").collect.toList)
  }
}
