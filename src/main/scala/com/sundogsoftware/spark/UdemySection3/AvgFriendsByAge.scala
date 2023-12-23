package com.sundogsoftware.spark.UdemySection3

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object AvgFriendsByAge extends App{

  val spark = SparkSession.builder.master("local[*]").appName("AvgFriendsByAge").getOrCreate
  Logger.getLogger("org").setLevel(Level.ERROR)

  var data =  spark.sparkContext.textFile("D:/Training/Spark-Scala/PractiseScalaSpark/data/fakefriends-noheader.csv")
  var dataRepart = data.repartition(5)
  var keyValue = dataRepart.map(ele => {
    var split = ele.split(",")
    (split(2).toInt, split(3).toInt)
  })
  var mapValues = keyValue.mapValues( y => (y, 1))
  var avgNoOfFriends =  mapValues.reduceByKey((first, second) => (first._1 + second._1, first._2 + second._2))
  var avgAge = avgNoOfFriends.mapValues(y => y._1/y._2)
  var avgAgeArray = avgAge.collect()
  var sorted =  avgAgeArray.toSeq.sortBy(_._1)
  sorted.foreach(println)
}
