package org.nuventosparktest

import org.apache.spark._
import org.apache.log4j._
/**
 * @author ${user.name}
 */
object App {
  

  
  def main(args : Array[String]):Unit= {
    println( "Hello World!" )
    val sc = new SparkContext("local[*]", "App")
    val lines = sc.textFile("data/customer-orders.csv")
  }

}
