package com.sundogsoftware.spark.UdemySection3.RatingsClass

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j._

object RatingsCounter extends App {
  val spark = SparkSession.builder.master("local[*]").appName("RatingsCounter").getOrCreate

  Logger.getLogger("org").setLevel(Level.ERROR)
  //  import spark.implicits._
  var data = spark.sparkContext.textFile("D:/Training/Spark-Scala/PractiseScalaSpark/data/ml-100k/u.data")
  var ratings = data.map(ele => ele.split("\t")(2))
  val results = ratings.countByValue()
  var sortedResults = results.toSeq.sortBy(_._2)
  sortedResults.foreach(println)
  //  data.count
//  data.map(ele => ele * 2)

//  val rdd = spark.sparkContext.parallelize(Seq(("A",1),("A",3),("B",4),("B",2),("C",5)))
//  rdd.filter(ele => ele._1 != 1)
//  rdd.groupBy(ele => ele.)
//  rdd.groupByKey()
//  rdd.coalesce()
// var data1 = spark.read.option("header", "true").format("csv").load("/D:/Training/Spark-Scala/data/input/movieRatings.csv").rdd
// var c =  data1.groupBy(ele => ele.get(0))
//  data1.groupBy()
//  var ab = data1.repartition(1000).toDF()
// var lm =  ab.filter(col("Ratings")>1)
//  ab.rdd.map(ele => ele.getString(1)
//  c.collect()

    Thread.sleep(1000000);//For 1000 seconds or more

}
