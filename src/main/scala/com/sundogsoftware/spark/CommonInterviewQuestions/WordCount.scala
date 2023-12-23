package com.sundogsoftware.spark.Rishabh_Software_Assesment

import org.apache.spark.sql.SparkSession

object WordCount extends App {
  val spark = SparkSession.builder().master("local[*]").appName("WordCount").getOrCreate();
  val data = spark.sparkContext.textFile("D:/Training/Spark-Scala/PractiseScalaSpark/data/input.txt")
  val words = data.flatMap(x => x.split("\\W+"))
  words.count()
  val wordCounts = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
  val wordCountsSorted = wordCounts.map(x => (x._2, x._1)).sortByKey(false)

  // Print the results, flipping the (count, word) results to word: count as we go.
  for (result <- wordCountsSorted) {
    val count = result._1
    val word = result._2
    println(s"$word: $count")
  }
}
