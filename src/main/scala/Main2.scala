package org.itc.com

import org.apache.spark.SparkContext

object Main2 {
  def main(args: Array[String]): Unit = {
    println("Hello world!")

    val sc = new SparkContext(master= "local[1]", appName="AppName")
    //read a file
    val rdd1 = sc.textFile("data1.txt")
    val header = rdd1.first()
    val data = rdd1.filter(line => line !=header)
    val temperature = data.map(line => line.split(",")(2)toFloat)
    val maxTemp=temperature.max()
    val greaterThan50 = temperature.filter(temp => temp>=50)
    val count = greaterThan50.count()

    println(s"the maximum temperature is $maxTemp")
    println(s"$count")
  }
}