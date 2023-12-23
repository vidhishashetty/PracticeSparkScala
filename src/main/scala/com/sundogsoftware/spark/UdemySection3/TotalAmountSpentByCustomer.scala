package com.sundogsoftware.spark.UdemySection3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TotalAmountSpentByCustomer extends App{
  var spark = SparkSession.builder.master("local[*]").appName("LogicalPlan_PhysicalPlan").getOrCreate
  Logger.getLogger("org").setLevel(Level.ERROR)
  var data = spark.sparkContext.textFile("D:/Training/Spark-Scala/PractiseScalaSpark/data/customer-orders.csv")
  var arr = data.map(ele => ele.split(","))
  var mapCustAm = arr.map(ele => {
    var customId = ele(0)
    var amount = ele(2).toDouble
    (customId, amount)
  })
  var custAm = mapCustAm.reduceByKey(_+_).sortBy(ele => ele._2, false)
  var collect = custAm.collect()
  collect.foreach(println)
  Thread.sleep(1000000);//For 1000 seconds or more

}
