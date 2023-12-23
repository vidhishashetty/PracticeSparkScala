//Anonymus Function
import scala.collection.mutable.ListBuffer
    val oldNumbers = List(1, 2, 3)

  //Imperative Code
def double(input : List[Int]): List[Int] = {
  val doubleList = new ListBuffer[Int]()
  for (i <-  input) {
    doubleList += i * 2
  }
  doubleList.toList
}

val doubleNumbers = double(oldNumbers)
println(s"doubleNumbers = $doubleNumbers")


 //
  //Exercise: Double all the elements of oldNumbers
  // write high-level, functional code using higher-order functions and lambdas
    val newNumbers  = oldNumbers.map(ele => ele * 2)
    val newNumbersTwo  = oldNumbers.map(_ * 2)
    println(s"newNumbers = $newNumbers")
    println(s"newNumbersTwo = $newNumbersTwo")
