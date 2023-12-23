package com.sundogsoftware.spark.CommonInterviewQuestions

import org.apache.spark.sql.SparkSession

object Partition_Bucketing extends App {
  val spark = SparkSession.builder.master("local[4]").appName("Partiton_Bucketing").getOrCreate

    val dataOne = spark.read.format("csv")
      .option("header", "true")
      .load("D:/Training/Spark-Scala/PractiseScalaSpark/data/fakefriends.csv")

//    data.write.partitionBy("age").mode("append").option("header", "true").format("csv").save("D:/Training/Spark-Scala/PractiseScalaSpark/data/output/fakefriends")
   //Partiton Only
    dataOne.write.partitionBy("age").mode("append")
      .option("header", "true").format("parquet")
      .save("D:/Training/Spark-Scala/PractiseScalaSpark/data/output/fakefriends_partionBy")

  //Bucketing Only
    dataOne.write.bucketBy(5, "age").mode("append")
      .option("path", "D:/Training/Spark-Scala/PractiseScalaSpark/data/output/fakefriends_bucketBy")
      .format("csv").saveAsTable("fakeFriendBucketBy")

//  val data = spark.read.textFile("D:/Training/Spark-Scala/PractiseScalaSpark/data/ml-100k/u.data")
  //   data.write.mode(SaveMode.Overwrite)
  //     .option("header", "true").format("csv")
  //     .save( "D:/Training/Spark-Scala/PractiseScalaSpark/data/output/Udemy/Session3")
//  data.count

}
