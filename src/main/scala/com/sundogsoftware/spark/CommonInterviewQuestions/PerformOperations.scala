package com.sundogsoftware.spark.Rishabh_Software_Assesment

//1. Create a list using the provided data.
//
//2. Convert the list into a DataFrame adhering to the specified schema.
//
//3. Display the schema of the resulting DataFrame.
//
//4. Filter individuals whose age ranges from 1 to 50.
//
//5. Add a new column named `isYoung` which evaluates to True for ages under 40, and False otherwise.
//
//6. Calculate the average age of people based on their gender.
import org.apache.spark.sql.functions.{avg, col, lit, when}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}

object PerformOperations extends App {
  val spark = SparkSession.builder.master("local[*]").appName("LogicalPlan_PhysicalPlan").getOrCreate

  //1. Create a list using the provided data.
  var data : List[Row]= List(Row("Joe", "Smith", "M", 45),
                 Row("Jack", "Miller", "M", 23),
                 Row("Anna", "Wood", "F", 32))

  //2. Convert the list into a DataFrame adhering to the specified schema.
  var dataSchema = StructType(Array(StructField("firstName", StringType),
    StructField("lastName", StringType), StructField("Gender", StringType),
    StructField("AgeInInteger", IntegerType)))

  var items = spark.createDataFrame(spark.sparkContext.parallelize(data), dataSchema)

  //3. Display the schema of the resulting DataFrame.
  items.printSchema()
   //or
  items.schema

  //4. Filter individuals whose age ranges from 1 to 50.
 var ageFilter = items.filter(col("AgeInInteger") >=1 and col("AgeInInteger") <= 40)
  items.createOrReplaceTempView("items")
//  spark.sql("select * from items WHERE AgeInInteger BETWEEN 1 and 50")

  //5. Add a new column named `isYoung` which evaluates to True for ages under 40, and False otherwise.
var isYoungCol = items.withColumn("isYoung", when(col("AgeInInteger") >=1 and col("AgeInInteger")<=40,
                 lit("True").cast(BooleanType)).otherwise(lit("False").cast(BooleanType)))

  //6. Calculate the average age of people based on their gender.
  items.createOrReplaceTempView("items")
  spark.sql("select Gender, AVG(AgeInInteger) from  items GROUP BY Gender")

  isYoungCol.write.save()
}
