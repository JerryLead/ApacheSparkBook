package iterative

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.sql.SparkSession
/**
  * Computes the PageRank of URLs from an input file. Input file should
  * be in format of:
  * URL         neighbor URL
  * URL         neighbor URL
  * URL         neighbor URL
  * ...
  * where URL and their neighbors are separated by space(s).
  *
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * please refer to org.apache.spark.graphx.lib.PageRank
  *
  * Example Usage:
  * {{{
  * bin/run-example SparkPageRank data/mllib/pagerank_data.txt 10
  * }}}
  */
object SparkPageRank {


  def main(args: Array[String]) {

    val spark = SparkSession
      .builder.master("local")
      .appName("SparkPageRank")
      .getOrCreate()

    val iters = 2
    val lines = spark.sparkContext.parallelize(Array[String](
      ("2 1"),("4 1"),("1 2"),("6 3"),("7 3"),("7 6"),("6 7"),("3 7"), ("2 3"),
        ("3 5"), ("5 2"), ("4 7"), ("1 7"), ("5, 7")
    ))
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.collect()
    output.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))

    System.in.read()
    spark.stop()
  }
}
// scalastyle:on println

