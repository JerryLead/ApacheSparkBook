package basic.transfromation

import org.apache.spark.sql.SparkSession

object CombineByKeyExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("combine by key example")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    val inputRDD = sc.parallelize(Array[(Int, Char)](
      (1, 'a'), (2, 'b'), (2, 'k'), (3, 'c'), (4, 'd'), (3, 'e'),
      (3, 'f'), (2, 'g'), (2, 'h')
    ), 3)

    println("-----------input RDD--------")
    inputRDD.mapPartitionsWithIndex((pid, iter) => {
      iter.map(value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.combineByKey((v: Char) => {
      if (v == 'c') {
        v + "0"
      } else {
        v + "1"
      }
    }
      , (c: String, v: Char) => c + "+" + v, (c1: String, c2: String) => c1 + "_" + c2, 2)
    //    val resultRDD = inputRDD.combineByKey((v:Char)=>List(v), (c:List[Char],v:Char)=>v::c,(c1:List[Char],c2:List[Char])=>c1:::c2)
    println(resultRDD.toDebugString)
    println("-----------result RDD--------")
    resultRDD.mapPartitionsWithIndex((pid, iter) => {
      iter.map(value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
