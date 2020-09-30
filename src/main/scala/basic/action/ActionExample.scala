package basic.action

import org.apache.spark.sql.SparkSession

object ActionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("action example").master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    val inputRDD1 = sc.parallelize(Array[(Int,Char)](
      (3,'c'),(3,'f'),(1,'a'),(4,'d'),(1,'h'),(2,'b'),(5,'e'),(2,'g')
    ),3)
    val inputRDD2 = sc.parallelize(Array[(Int,Char)](
      (1,'A'),(2,'B'),(3,'C'),(4,'D'),(6,'E')
    ),2)
    val inputRDD3 = sc.parallelize(Array[(Int,String)](
      (1,"A"),(2,"B"),(3,"C"),(4,"D"),(6,"E")
    ),2)
    val inputRDD4 = sc.parallelize(Array[(Int, String)](
      (2,"2"),(1,"1"),(3,"3"),(4,"4"),(5,"5"),(3,"3"),(8,"8"),(6,"6")
    ),4)

    println("-------------input rdd1------------")
    inputRDD1.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println("-------------input rdd2------------")
    inputRDD2.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println("-------------input rdd3------------")
    inputRDD3.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)


    val resultRDD = inputRDD1.leftOuterJoin(inputRDD2,2)
    println(resultRDD.toDebugString)
    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println("input rdd1's dependencies' size: "+inputRDD1.dependencies.length)
    println("input rdd2's dependencies' size: "+inputRDD2.dependencies.length)
    println("input rdd1 left outer join with input rdd2 => dependencies' size: "+resultRDD.dependencies.length)
    println("input rdd1 left outer join with input rdd2 => dependencies: "+resultRDD.dependencies)
    println("input rdd1 left outer join with input rdd2 => dependencies: "+resultRDD.dependencies(0).rdd.collect().toList)
    println("input rdd2 left outer join with input rdd3 => dependencies: "+inputRDD2.leftOuterJoin(inputRDD3).dependencies)

    println("------------collect result------no toArray")
    println(resultRDD.collect().toList)
    println("------------collect as map result------")
    println(resultRDD.collectAsMap().toList)
    println("------------reduce by key locally result------")
    val reduceByKeyLocallyResultRDD = resultRDD.reduceByKeyLocally((x,y) => y)
    reduceByKeyLocallyResultRDD.foreach(_=>println)
    for((key, value) <- reduceByKeyLocallyResultRDD){
      println(key+" : "+value)
    }
    println("------------lookup result------")
    println(resultRDD.lookup(1))
    println(inputRDD1.lookup(1))
    println("------------count result------")
    println(resultRDD.count())
    println("------------top 3 result------")
    println(resultRDD.top(3).toList)
    println(inputRDD1.top(3).toList)
    println("------------take 2 result------")
    println(resultRDD.take(2).toList)
    println(inputRDD1.take(2).toList)
    println("------------take ordered 2 result------")
    println(resultRDD.takeOrdered(2).toList)
    println(inputRDD1.takeOrdered(2).toList)
    println("------------first result------")
    println(resultRDD.first())
    println(inputRDD1.first())
    println("------------reduce (left) result------")
    println(inputRDD3.reduce((A,B)=>(A._1+B._1, A._2+"@"+B._2)))
    println("------------tree reduce result------")
    println(inputRDD3.treeReduce((A,B)=>(A._1+B._1, A._2+"@"+B._2)))
    println("------------folder result------")
    println(inputRDD3.fold((2,"O"))((A,B)=>(A._1+B._1, A._2+"@"+B._2)))
    println("------------aggregate result------")
    println(inputRDD3.aggregate((2,"O"))((A,B)=>(A._1+B._1, A._2+"@"+B._2),(A,B)=>(A._1+B._1, A._2+"+"+B._2)))
    println(inputRDD3.aggregate(2)((A,B)=>A+B._1,(A,B)=>(A+B)))
    println("------------tree aggregate result------")
    println(inputRDD3.treeAggregate((2,"O"))((A,B)=>(A._1+B._1, A._2+"@"+B._2),(A,B)=>(A._1+B._1, A._2+"+"+B._2)))
    println(inputRDD3.treeAggregate(2)((A,B)=>A+B._1,(A,B)=>(A+B)))
    println(inputRDD3.treeAggregate((4,"O"))((A,B)=>(math.max(A._1,B._1), A._2+"@"+B._2),(A,B)=>(A._1+B._1, A._2+"+"+B._2)))
    println(inputRDD4.treeAggregate((4,"O"))((A,B)=>(math.max(A._1,B._1), A._2+"@"+B._2),(A,B)=>(A._1+B._1, A._2+"+"+B._2)))
    println(inputRDD4.treeAggregate((4,"O"))((A,B)=>(math.max(A._1,B._1), A._2+"@"+B._2),(A,B)=>(A._1+B._1, A._2+"+"+B._2),3))

    println("------------preferredLocations result------")
    println(resultRDD.dependencies(0).rdd.preferredLocations(resultRDD.partitions(1)))
    println("------------max result------")
    println(inputRDD1.max())
    println("------------id result---------")
    println(inputRDD1.id)
    println("------------values result---------")
    println(inputRDD1.values.collect().toList)
  }
}
