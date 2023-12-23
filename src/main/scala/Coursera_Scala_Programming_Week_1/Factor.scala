package Coursera_Scala_Programming_Week_1

object Factor extends App {



    //Without Tail Recurrsion
    def factorialNoTail(x: Int): Int =
      if (x == 0) 1 else x * factorialNoTail(x - 1)

      //With Tail Recursion
    //Return Types for Recursive methods are Compulsary
    def factorialTR(x: Int, a: Int) : Int = {
        if (x == 1) a
        else factorialTR(x - 1, x * a)
      }

    //Return Types for Non Recursive methods are optional
      def factorial(x : Int) = {
        factorialTR(x, 1)
      }

    println(s"Tail Reculsive  = ${factorial(5)}")
    println(s"Non Tail Reculsive  = ${factorialNoTail(5)}")

}

