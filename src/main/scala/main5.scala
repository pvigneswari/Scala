package org.itc.com

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object main5 {
  def main(args: Array[String]): Unit = {
    println("Hello vigneswari!")


    // Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("Data_cleaning")
      .enableHiveSupport()
      .getOrCreate()
    println(spark)
    println("Hello Rajesh!")
  }

}
