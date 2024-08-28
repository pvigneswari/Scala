package org.itc.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}
import org.itc.com.sparksqldemo.spark

object joinDemo extends App {


  Logger.getLogger( "org").setLevel(Level.ERROR)

  val sparkConf =new SparkConf()
  sparkConf.set("spark.app.name","DataframeDemo")
  sparkConf.set("spark.master","local[*]")
  //val ddlschema = """product_number"""
  val spark = SparkSession.builder().config(sparkConf).getOrCreate();
  //explicit schema way1
  val productschema = StructType(Array(
    StructField("product_number",StringType,nullable = false),
    StructField("product_name",StringType,nullable = false),
    StructField("product_category",StringType,nullable = false),
    StructField("product_scale",StringType,nullable = false),
    StructField("product_manufacturer",StringType,nullable = false),
    StructField("product_description",StringType,nullable = false),
    StructField("length", FloatType,nullable = false),
    StructField("width", FloatType,nullable = false),
    StructField("height", FloatType,nullable = false)
  ))

  //explicit schema way2 - ddl string

  val ddlschema=
    """product_number string, product_name string,product_category string, product_scale string,
      | product_manufacturer string, product_description string, length double, width double, height float""".stripMargin


  //  val productdf = spark.read.option("header",true).option("inferSchema",true).csv(path = "C:\\Users\\vigup\\Training\\Scala\\warehouse\\products.csv")
  val productdf = spark.read.option("header", true).schema(productschema).csv("C:\\Users\\vigup\\Training\\Scala\\warehouse\\clean\\productscleaned1\\")

  val orderitemschema = """order_number string,product_number string,product_category string,price int, quantity int"""
  val oitemdf = spark.read.option("header", true).schema(orderitemschema).csv("C:\\Users\\vigup\\Training\\Scala\\warehouse\\orderdetails.csv")
  val resdf = productdf.join(oitemdf,productdf.col("product_number") === oitemdf.col("product_number"))

  //join using spark sql
//  spark.sql("select order_items.product_number,order_items.order_number,products.product_category from products inner join order_items on products.product_number = order_items.product_number").show(5)
  resdf.show(5)
}

