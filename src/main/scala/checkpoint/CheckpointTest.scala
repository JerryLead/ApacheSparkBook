package checkpoint

import org.apache.spark.sql.SparkSession

object CheckpointTest {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Checkpoint Test")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setCheckpointDir("/Users/xulijie/Documents/data/checkpoint2")

    val data = Array[(Int, Char)]((1, 'a'), (2, 'b'),
      (3, 'c'), (4, 'd'),
      (5, 'e'), (3, 'f'),
      (2, 'g'), (1, 'h')
    )
    val pairs = sc.parallelize(data, 3).map(r => (r._1 + 10, r._2))
    // pairs.cache()
    pairs.checkpoint()
    // pairs.count




//    pairs.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)

    val result = pairs.groupByKey(2)



//    result.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)
    //result.checkpoint()
    result.foreach(println)


    println(result.toDebugString)

    System.in.read()

  }
}
