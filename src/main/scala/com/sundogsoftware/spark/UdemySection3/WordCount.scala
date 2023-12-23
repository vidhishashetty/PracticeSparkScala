package com.sundogsoftware.spark.UdemySection3

import com.sundogsoftware.spark.UdemySection3.RatingsClass.RatingsCounter.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordCount extends App{
  var spark = SparkSession.builder.master("local[*]").appName("LogicalPlan_PhysicalPlan").getOrCreate
  Logger.getLogger("org").setLevel(Level.ERROR)
  var data = spark.sparkContext.textFile("D:/Training/Spark-Scala/PractiseScalaSpark/data/book.txt")
  var multipleSeq = data.flatMap(ele => ele.split("\\W+"))
  var collectAction = multipleSeq.countByValue().toSeq
  collectAction.foreach(println)
  Thread.sleep(1000000);//For 1000 seconds or more


}
