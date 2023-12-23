package com.sundogsoftware.spark.CommonInterviewQuestions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ThirdHighestSalary_window {
  val spark = SparkSession.builder().master("local[*]").appName("SparkExamples").getOrCreate()

  import spark.implicits._

  // Find the third highest salary in each department
  //Begin
  val simpleData = Seq(("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  )
  val df = simpleData.toDF("employee_name", "department", "salary")

  val windowsSpec = Window.partitionBy("department").orderBy("salary")
  val c = df.withColumn("Rank", dense_rank().over(windowsSpec)).filter(col("Rank") === 3)
  c.show
  // End
}
