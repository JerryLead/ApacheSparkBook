package basic.transfromation

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object ZipPartition3Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("zip partition example")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

//    var inputRDD1 = sc.parallelize(1 to 9,3)
//    inputRDD1 = inputRDD1.repartitionAndSortWithinPartitions(new HashPartitioner(3))
//    val inputRDD2 = sc.parallelize('a' to 'j',3)
//    val inputRDD2 = sc.parallelize('a' to 'k',3)
    var inputRDD1 = sc.parallelize(Array[(Int, Char)](
      (1,'a'),(1,'b'),(2,'c'),(3,'d'),(4,'e'),(5,'f')
    ),3)
//    inputRDD1 = inputRDD1.repartitionAndSortWithinPartitions(new HashPartitioner(3))
    var inputRDD2 = sc.parallelize(Array[(Int, Char)](
      (1,'f'),(3,'g'),(2,'h')
    ),3)
    println("-------------input rdd1------------")
    inputRDD1.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println("-------------input rdd2------------")
    inputRDD2.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD1.zipPartitions(inputRDD2,preservesPartitioning=true)({
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
