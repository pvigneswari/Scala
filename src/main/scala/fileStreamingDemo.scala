package org.itc.com
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.{SparkConf, SparkContext}

object fileStreamingDemo extends App {

  Logger.getLogger( "org").setLevel(Level.ERROR)
  val sparkConf =new SparkConf()
  sparkConf.set("spark.app.name","DataframeDemo")
  sparkConf.set("spark.master","local[*]")
  sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")
  sparkConf.set("spark.sql.streaming.schemaInference", "true")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate();
  val ddlschema = """order_id int,order_date string, order_customer_id int, order_status string, amount int"""

  val ordersdf = spark.readStream.format("json").schema(ddlschema).option("path", "input").load()

  ordersdf.createOrReplaceTempView("order")
  val onholddata = spark.sql("select * from order where order_status = 'ON_HOLD' ")
  val res = onholddata.writeStream.format("json").option("path", "output").outputMode("append")
    .option("checkpointLocation","checkpoint-location")
    .trigger(Trigger.ProcessingTime("20 seconds"))
    .start()

  res.awaitTermination()
}
