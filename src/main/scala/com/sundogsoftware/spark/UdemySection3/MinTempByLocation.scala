package com.sundogsoftware.spark.UdemySection3

import org.apache.spark.sql.SparkSession
import scala.math.min

object MinTempByLocation extends App {
  var spark = SparkSession.builder.master("local[*]").appName("MinTempByLocation").getOrCreate
  var data = spark.sparkContext.textFile("D:/Training/Spark-Scala/PractiseScalaSpark/data/1800.csv")
  var split=data.map(ele => {
    var arr = ele.split(",")
    var stationID = arr(0)
    var tempType = arr(2)
    var temp = arr(3)
    (stationID, tempType, temp)
  }).filter(_._2 == "TMIN").map(tuple => (tuple._1, tuple._3.toFloat * 0.1))
  var minTemp = split.reduceByKey((x,y) => min(x, y))
  var results = minTemp.collect()
  for(result <- results.sorted){
    var stationName = result._1
    var minTemp = result._2
    var formatedTemp = f"$minTemp%.2f Degree"
    println(s"Minimum Temp at $stationName = $formatedTemp")
    Thread.sleep(1000000);//For 1000 seconds or more
  }
}
