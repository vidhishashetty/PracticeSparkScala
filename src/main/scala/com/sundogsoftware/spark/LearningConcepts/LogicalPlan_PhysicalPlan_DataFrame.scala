package com.sundogsoftware.spark.LearningConcepts
//https://medium.com/datalex/sparks-logical-and-physical-plans-when-why-how-and-beyond-8cd1947b605a
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StringType, StructField, DoubleType, IntegerType}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._

object LogicalPlan_PhysicalPlan_DataFrame extends App {
  val spark = SparkSession.builder.master("local[*]").appName("LogicalPlan_PhysicalPlan").getOrCreate
  var itemsData = Seq(
    Row(0, "Tomato", 2.0),
    Row(1, "Watermelon", 5.5),
    Row(2, "Pineapple", 7.0),
    Row(3, "Mango", 5.0),
    Row(4, "Plum", 15.0))

  var itemsSchema = StructType(Array(StructField("itemId", IntegerType, true),
    StructField("name", StringType, true), StructField("price", DoubleType, true)))

  var items = spark.createDataFrame(spark.sparkContext.parallelize(itemsData), itemsSchema)

  var ordersSchema = StructType(Array(StructField("orderId", IntegerType),
    StructField("itemId", IntegerType), StructField("qty", IntegerType)))

  var ordersData = Seq(
    Row(100, 0, 1),
    Row(100, 1, 1),
    Row(101, 2, 3),
    Row(102, 2, 8))

  var orders = spark.createDataFrame(spark.sparkContext.parallelize(ordersData), ordersSchema)

  //Check the toStringDebug(), Logical Partitong, Physical Partitiong, DAG using both conditions
  //   spark.conf.set("spark.sql.codegen.wholeStage", false)
  spark.conf.set("spark.sql.codegen.wholeStage", false)
  spark.conf.set("spark.sql.adaptive.enabled", true)


  //  Applying SQL like  functions
    var join = items.as("it")
      .join(orders.as("ord"), col("it.itemId") === col("ord.itemId"), "inner")
      .groupBy(col("it.name").as("name"))
      .agg(first("it.itemId").as("itemId"),
        first("it.price").as("price"),
        first("ord.orderId").as("orderId"),
        sum("ord.qty").as("totalQty"))
      .where("itemId==2")
      .repartition(5)


  //Create temp View from DF, so that we can Run SQL query against it
  items.createOrReplaceTempView("itemsView")
  orders.createOrReplaceTempView("ordersView")

//  // Using SQL query
//  var join = spark.sql("select /*+ REPARTITION(3) */ first(it.itemId) as itemId, first(it.name) as name, first(it.price)" +
//    " as price, first(ord.orderId) as orderId, SUM(ord.qty) as totalQty from itemsView it " +
//    "INNER JOIN ordersView ord ON it.itemId=ord.itemId " +
//    "WHERE ord.orderId is NOT NULL AND it.itemId = 2 " +
//    "GROUP BY name")

//  var join = spark.sql("select first(it.itemId) as itemId, it.name as name, first(it.price)" +
//    " as price, first(ord.orderId) as orderId, SUM(ord.qty) as totalQty  from itemsView it " +
//    "LEFT JOIN ordersView ord ON it.itemId==ord.itemId " +
//    "GROUP BY name")

  val reOrderedColumnName: Array[String] = Array("itemId", "name", "price", "orderId", "totalQty")
  val orderejoindDS = join.select(reOrderedColumnName.head, reOrderedColumnName.tail: _*)
  orderejoindDS.show
  orderejoindDS.printSchema
  orderejoindDS.explain(true)
  println(orderejoindDS.rdd.toDebugString)

  println("spark.sql.adaptive.enabled : " + spark.conf.get("spark.sql.adaptive.enabled"))
  println("spark.sql.autoBroadcastJoinThreshold : " + spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
  println("spark.sql.join.preferSortMergeJoin : " + spark.conf.get("spark.sql.join.preferSortMergeJoin"))
  println(spark.version)
  //Check DAG too
  Thread.sleep(1000000);//For 1000 seconds or more
  spark.stop()
}




