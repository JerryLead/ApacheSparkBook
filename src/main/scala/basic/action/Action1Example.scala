package basic.action

import org.apache.spark.sql.SparkSession

object Action1Example {
  val myfunc: PartialFunction[Any, Any] = {
    case a: Int    => "is integer"
    case b: String => "is String"
    case c: Float => "is Float"
  }

  def seq(a:Int,b:Int):Int={
    println("seq:"+a+":"+b)
    math.min(a,b)
  }
  def comb(a:Int,b:Int):Int={
    println("comb:"+a+":"+b)
    a+b
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("action1 example").master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(List(2,1,3,4,5,3,8,6),3)
    val inputRDD1 = sc.parallelize(1 to 1000,3)
    val inputRDD3 = sc.parallelize(List(0.2,1,5,9,"Hi",4,"5"))
    val dataRDD0 = sc.parallelize(List("A","B","C","D"),2)
    val dataRDD1 = sc.parallelize(dataRDD0.takeSample(true,10000,0),20)
    val dataRDD2 = sc.parallelize(1 to dataRDD1.count().toInt,20)
    val dataRDD = dataRDD1.zip(dataRDD2)
    println("-------------input rdd------------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    println("------------collect result------")
    println(inputRDD.collect().toList)
    println("------------count result------")
    println("origin:    "+inputRDD.count())
    println("------------countApprox result------2,0.95")
    println(inputRDD.countApprox(2,0.95).getFinalValue())
    println("origin:    "+inputRDD1.count())
    println(inputRDD1.countApprox(2,0.95))
    println(inputRDD1.countApproxDistinct(0.9))
    println(inputRDD1.countApproxDistinct(0.5))
    println(inputRDD1.countApproxDistinct(0.1))
    println(inputRDD1.countApproxDistinct(0.001))
    println("origin:    "+dataRDD.count())
    println("origin:    "+dataRDD.countByKey())
    println(dataRDD.countByKeyApprox(2,0.01).getFinalValue())
    println(dataRDD.countApproxDistinctByKey(0.9).collect().toList)
    println(dataRDD.countApproxDistinctByKey(0.5).collect().toList)
    println(dataRDD.countApproxDistinctByKey(0.1).collect().toList)
    println(dataRDD.countApproxDistinctByKey(0.001).collect().toList)
    println("origin:     "+dataRDD.map(obj => obj._1).countByValue())
    println(dataRDD.map(obj => obj._1).countByValueApprox(2,0.01).getFinalValue())
    println("------------top 3 result------")
    println(inputRDD.top(3).toList)
    println("------------take 2 result------")
    println(inputRDD.take(2).toList)
    println("------------take ordered 2 result------")
    println(inputRDD.takeOrdered(2).toList)
    println("------------first result------")
    println(inputRDD.first())
    println("------------folder result------")
    println(inputRDD.fold(2)(_+_))
    println("------------aggregate result------")
    println(inputRDD.aggregate(4)(math.max(_,_),_+_))
    println("------------tree aggregate result------")
    println(inputRDD.treeAggregate(4)(math.max(_,_),_+_))
    println("------------filter result---------")
    println(inputRDD.filter(_>3.0).collect().toList)
    println(inputRDD3.collect({
      case a:Int => "is iterger"
      case b:String => "is String"
      case c:Float => "is Float"
    }).collect().toList)
    println(myfunc.isDefinedAt(""))
    println(myfunc.isDefinedAt(1))
    println(myfunc.isDefinedAt(2.2))
    println("------------foreach partition result---------")
    inputRDD.foreachPartition(x=>println(x.reduce(_+_)))
    println("------------histogram result---------")
    val histogramRst = sc.parallelize(List(1.1,1.2,1.3,2.0,2.1,7.4,7.5,7.6,8.8,9.0),3).histogram(5)
    println(histogramRst._1.toList)
    println(histogramRst._2.toList)
    val histogramRst1 = sc.parallelize(List(1.1,1.2,1.3,2.0,2.1,7.4,7.5,7.6,8.8,9.0),3).histogram(Array(1.0, 2.0, 3.0))
    println(histogramRst1.toList)
    println("------------id result---------")
    println(inputRDD.id)
    println(inputRDD3.id)
    println(dataRDD.id)
    println("------------values result---------")
//    println(dataRDD.values.collect().toList)
    println("------------max result------")
    println(inputRDD1.max())
//    println(inputRDD3.max())
    println("------------min result------")
    println(inputRDD.min())
    println("------------mean result------")
    println(inputRDD.mean())
    println(inputRDD.meanApprox(2,0.001).getFinalValue())
    println("------------name result------")
    println(inputRDD.name)
    println("------------partitions result------")
    println(inputRDD.partitions.length)
    for(item <- inputRDD.partitions){
      println(item.toString)
    }
    println("------------reduce result------")
    println(inputRDD.reduce((x,y) => x+y))
    println("------------tree reduce result------")
    println(inputRDD.treeReduce((x,y) => x+y))
    println("------------stats result------")
    println(inputRDD.stats())
    println("------------stdev result------")
    println(inputRDD.stdev())
    println("------------variance result------")
    println(inputRDD.variance())
    println("------------sample variance result------")
    println(inputRDD.sampleVariance())
    println("------------sum result------")
    println(inputRDD.sum())
    println("------------sumApprox result------")
    println(inputRDD.sumApprox(2,0.001).getFinalValue())
    println("------------toJavaRDD result------")
    println(inputRDD.toJavaRDD())
    println("------------toLocalIterator result------")
    val iter = inputRDD.toLocalIterator
    while(iter.hasNext){
      println(iter.next())
    }
    println("------------toString result------")
    println(inputRDD.toString())
    println(dataRDD.toString())
    println("------------aggregate-------------")
    val z =sc.parallelize(List(1,2,4,5,8,9),3)
    println(z.aggregate(3)(seq,comb))
    println("------------tree aggregate--------")
    println(z.treeAggregate(3)(seq,comb))
    println("------------tree aggregate------2--")
    println(z.treeAggregate(3)(seq,comb),2)
    println("------------tree aggregate------1--")
    println(z.treeAggregate(3)(seq,comb),1)
    println("------------tree aggregate------3--")
    println(z.treeAggregate(3)(seq,comb),3)
  }
}
