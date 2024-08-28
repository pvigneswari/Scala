package org.itc.com

import org.apache.commons.collections.functors.TruePredicate
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object Main {
  def main(args: Array[String]): Unit = {
    //println("Hello world!")

    Logger.getLogger("org").setLevel(Level.ERROR)
    val scf = new SparkConf()
    scf.setAppName("firstdemo")
    val sc = new SparkContext(scf)
    // val sc = new SparkContext(master = "local[1]", appName = "AppName")
    //read a file
    val rdd1 =sc.textFile(args(0))
    // val rdd1 = sc.textFile( path = "data.txt")
    //read line from the file, split by space, 1 line will give many words , 1to M
    val words = rdd1.flatMap(line => line.split(" "))


    val word_count = words.map(w => (w,1))
    //val result = word_count.reduceByKey((x, y) => x + y).sortByKey(true)
    //val result = word_count.reduceByKey((x, y) => x + y).sortBy(_._2, true)
    //val result = word_count.reduceByKey((x, y) => x + y).map(x => (x._2,x._1)).sortByKey()
    val result = word_count.countByKey()

    // result.collect().foreach(println)
    println(result)

  }
}