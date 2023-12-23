package com.sundogsoftware.spark.Rishabh_Software_Assesment

import org.apache.spark.sql.SparkSession

object OptimizaSparkApplication extends App{
  val spark = SparkSession.builder.master("local[*]").appName("LogicalPlan_PhysicalPlan").getOrCreate
  val df = spark.read.parquet("dbfs:/mnt/training/weather/StationData/stationData.parquet")
  df.show(10, false)

}
