package com.sundogsoftware.spark.UdemySection5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}


object MostPopularMovie extends App{
  val spark = SparkSession.builder.master("local[*]").appName("MostPopularMovie").getOrCreate
  spark.conf.set("spark.sql.codegen.wholeStage", false)


  case class MovieLens(userId : Int, movieID : Int, Ratings : Int , timestamp : Long)

  import spark.implicits._

  var movieLensSchema = StructType(List(StructField("userId", IntegerType, true),
      StructField("movieId", IntegerType, true),
      StructField("Ratings", IntegerType, true),
      StructField("timeStamp", LongType, true)))

  var movieDetailsSchema = StructType(List(StructField("movieId", IntegerType, true),
    StructField("movieName", StringType, true),
    StructField("ReleaseDate", StringType, true)))

  val movieLens = spark.read.format("csv").option("sep", "\t").schema(movieLensSchema).load("D:/Training/Spark-Scala/PractiseScalaSpark/data/ml-100k/u.data").as[MovieLens]

  val movieDetails = spark.read.format("csv").option("sep", "|").schema(movieDetailsSchema).load("D:/Training/Spark-Scala/PractiseScalaSpark/data/ml-100k/u.item")

  movieLens.createOrReplaceTempView("movieLens")
  movieDetails.createOrReplaceTempView("movieDetails")

  var df = spark.sql("select first(md.movieId) as movieId, first(md.movieName), COUNT(ml.userId) as viewers, round(AVG(ml.ratings),2) as avgRating " +
    "from movieLens as ml " +
    "INNER JOIN " +
    "movieDetails as md  WHERE ml.movieId == md.movieId GROUP BY ml.movieId ORDER BY viewers DESC, avgRating DESC")

//  var df = spark.sql("select md.movieId as movieId, md.movieName from movieLens as ml " +
//    "INNER JOIN " +
//    "movieDetails as md  WHERE ml.movieId == md.movieId")
//
//  var df = spark.sql("select ml.movieId as movieId " +
//    "from movieLens as ml GROUP BY ml.movieId " +
//    "ORDER BY viewers DESC")
  df.explain(true)
  println(df.rdd.toDebugString)

  println("spark.sql.adaptive.enabled : " + spark.conf.get("spark.sql.adaptive.enabled"))
  println("spark.sql.autoBroadcastJoinThreshold : " + spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
  println("spark.sql.join.preferSortMergeJoin : " + spark.conf.get("spark.sql.join.preferSortMergeJoin"))
  df.show
  Thread.sleep(1000000);//For 1000 seconds or more
spark.stop()
}
