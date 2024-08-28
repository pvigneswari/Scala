package org.itc.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.itc.com.joinDemo.sparkConf

object streamingDemo extends App {

  Logger.getLogger( "org").setLevel(Level.ERROR)
  val sparkConf =new SparkConf()
  sparkConf.set("spark.app.name","DataframeDemo")
  sparkConf.set("spark.master","local[*]")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate();
  val lines = spark.readStream.format("socket").option("host","localhost").option("port",9996).load()
  import spark.implicits._
  val words = lines.as[String].flatMap(_.split(" "))
  val wordcnt = words.groupBy(("value")).count
  val producer1 =wordcnt.writeStream.outputMode("complete").format("console")
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .option("checkpointlocation","checkpoint-location").start()
  //    val sc = new SparkContext("local[*]","streamdemo")
//  val ssc = new StreamingContext(sc,Seconds(2))
//  val lines = ssc.socketTextStream("localhost", 9996)
//  val wordcnt = lines.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey(((x,y) => (x+y)))
//  wordcnt.print()
//  ssc.start()
//  ssc.awaitTermination()
  producer1.awaitTermination()

}
