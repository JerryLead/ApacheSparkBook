package basic.action

import org.apache.spark.sql.SparkSession

object TreeAggregateExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("reduce by key example")
      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext


    var inputRDD = sc.parallelize(1 to 18,6).map(x => x + "")


    val result = inputRDD.treeAggregate("0")((x,y) => x + "_" + y, (x,y) => x + "=" + y)
    println(result)

    val result2 = inputRDD.treeReduce((x, y) => x + "_" + y)
    println(result2)

    System.in.read()

  }
}
