package org.itc.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object main4 {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext(master= "local[1]", appName="AppName")
    //read a file
    val rdd1 = sc.textFile("data.txt")

    //read line from the file, split by space, 1 to M , 1 line will give you many words
    val words = rdd1.flatMap(line => line.split(" "))

    //every word we count , 1 to 1 , input : hello , output : (hello, 1)
    val word_count = words.map(w => (w.toLowerCase(),1))
    //aggregation using the keys  from the tuples(w)
   // val result = word_count.reduceByKey((x, y) => x+y).map(x => (x._2,x._1)).sortByKey()
    val result = word_count.countByKey()
   // result.collect().foreach(println)
    result.foreach(println)
    println(result)
  }
}