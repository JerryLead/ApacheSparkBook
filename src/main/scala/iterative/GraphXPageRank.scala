package iterative

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

object GraphXPageRank {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder.master("local[2]")
      .appName("SparkPageRank")
      .getOrCreate()

    val sc = spark.sparkContext

    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt")
    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices
    // Join the ranks with the usernames
    val users = sc.textFile("data/graphx/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))


    System.in.read()
    spark.stop()
  }

}





