package com.sundogsoftware.spark.CommonInterviewQuestions

import org.apache.spark.sql.SparkSession


object Repartition_Coalesce extends App {
    val spark = SparkSession.builder.master("local[4]").appName("Repartition").getOrCreate

  import spark.implicits._

    val rdd = spark.sparkContext.textFile("D:/Training/Spark-Scala/PractiseScalaSpark/data/ml-100k/u.data").toDF()
    println("Initial partition, From local[5] " + rdd)
//    println("Get No of partitons " + rdd.getNumPartitions)
//    val abc = rdd.map(ele => (ele.getString(2), ele.getString(1))).groupByKe


}
