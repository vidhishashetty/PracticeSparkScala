package com.sundogsoftware.spark

object LearningScala2 {
  def main(args: Array[String]) {
    ///Anonymus Function

    import scala.collection.mutable.ListBuffer

    val oldNumbers = List(1, 2, 3)

    //Imperative Code
    def double(input: List[Int]): List[Int] = {
      val doubleList = new ListBuffer[Int]()
      for (i <- input) {
        doubleList += i * 2
      }
      doubleList.toList
    }

    val doubleNumbers = double(oldNumbers)
    println(s"doubleNumbers = $doubleNumbers")


    //
    //Exercise: Double all the elements of oldNumbers
    // write high-level, functional code using higher-order functions and lambdas
    val newNumbers = oldNumbers.map(ele => ele * 2)
    val newNumbersTwo = oldNumbers.map(_ * 2)
    println(s"newNumbers = $newNumbers")
    println(s"newNumbersTwo = $newNumbersTwo")

    ///Option in scala [Some, None]
    val grades = Map("John" -> 8, "Sam" -> 6)

    val grade_john  = grades.get("John")
    val grade_gaby = grades.get("Gaby")

    println("John's grade - " + grade_john)
    println("Gaby's grade - " + grade_gaby)

    //Iterables
    val it = Iterable
  }

}
