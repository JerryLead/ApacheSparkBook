package basic.transfromation

import org.apache.spark.sql.SparkSession

object ZipPartitionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("zip partition example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputRDD1 = sc.parallelize(1 to 9,3)
//    val inputRDD2 = sc.parallelize('a' to 'j',3)
    val inputRDD2 = sc.parallelize('a' to 'k',3)


    println("-------------input rdd1------------")
    inputRDD1.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println("-------------input rdd2------------")
    inputRDD2.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    val resultRDD = inputRDD1.zipPartitions(inputRDD2,preservesPartitioning=false)({
      (rdd1Iter, rdd2Iter) =>{
        var result = List[String]()
        while(rdd1Iter.hasNext && rdd2Iter.hasNext){
          result ::= rdd1Iter.next()+"_"+rdd2Iter.next()
        }
        result.iterator
      }
    })
    println(resultRDD.toDebugString)
    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
