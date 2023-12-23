package com.sundogsoftware.spark.CommonInterviewQuestions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.types.{StructType, StringType, StructField, ArrayType, MapType, DateType}
import org.apache.spark.sql.functions.col
object Explode_Function extends App{
  var spark = SparkSession.builder().master("local[*]").appName("Explode").getOrCreate()
  val arrayData = Seq(
    Row("James", List("Java", "Scala"), Map("hair" -> "black", "eye" -> "brown"), "2012-02-01", "5"),
    Row("Michael", List("Spark", "Java", null), Map("hair" -> "brown", "eye" -> null),  "2009-01-20", "4"),
    Row("Robert", List("CSharp", ""), Map("hair" -> "red", "eye" -> ""), "2007-06-23", "8"),
    Row("Washington", null, null, null, "2"),
    Row("Jefferson", List(), Map(),"", "10")
  )
  var arraySchema = new StructType()
    .add("name", StringType, false)
    .add("languages", ArrayType(StringType, true), true)
    .add("properties", MapType(StringType,StringType, true), true)
    .add("date", StringType, true)
    .add("increment", StringType, false)

  var schema = StructType(
    List(StructField("name", StringType, false),
      StructField("languages", ArrayType(StringType), true),
      StructField("properties", MapType(StringType, StringType))))

   var df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData), arraySchema)
//    df.show(false)
  //  var selectExpr = df.selectExpr("name","languages", "properties")
  //  var select = df.select("name","languages", "properties")
  //               df.select(col("name"), col("languages"), col("properties"))
  //               df.select($"name", $"languages", $"properties")

  var addMonths = df.selectExpr("name","languages", "properties", "date", "add_months(to_date(date,'yyyy-MM-dd'),cast(increment as int)) as newDates")
//  addMonths.show(false)
  var posexplodeArray = addMonths.selectExpr("name", "posexplode_outer(languages) as (posArray, languages)", "properties", "date", "newDates")
  var posexplodeMap = posexplodeArray.selectExpr("name", "posArray", "languages", "posexplode_outer(properties) as(posMap, mapKey, mapValue)", "date", "newDates")
  posexplodeArray.show(false)
//  posexplodeMap.show(false)
  posexplodeMap.printSchema()

 println(posexplodeMap.rdd.toDebugString)
  println(posexplodeMap.explain(true))


  Thread.sleep(1000000);//For 1000 seconds or more

}
