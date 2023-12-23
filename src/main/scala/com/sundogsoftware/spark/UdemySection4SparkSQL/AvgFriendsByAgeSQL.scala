package com.sundogsoftware.spark.UdemySection4SparkSQL

import org.apache.spark.sql.SparkSession

object AvgFriendsByAgeSQL extends App {
  val spark = SparkSession.builder.master("local[*]").appName("AvgFriendsByAgeSQL").getOrCreate
  case class Person(id : Int, name : String,  age : Int, friends : Int)

  import spark.implicits._
  var friendsDetails= spark.read.format("csv")
    .option("header", "true").option("inferSchema", "true")
    .load("D:/Training/Spark-Scala/PractiseScalaSpark/data/fakefriends.csv")
    .as[Person]

  // Find Avg No.of friends of a particular Age using SparkSQL
  friendsDetails.createOrReplaceTempView("friendsDetailsView")
    var a = spark.sql("select age, round(AVG(friends), 2) as friends_avg from friendsDetailsView GROUP BY age")
    a.show
    Thread.sleep(1000000);//For 1000 seconds or more
    spark.stop()
}
