package org.itc.com
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lower, rank, trim, when}
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField, StructType}


object DFDemo_transformation extends App {

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
//    StructField("product_scale",StringType,nullable = false),
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
 var selecteddf = productdf.select("product_number", "product_description","length","product_category","product_name")
   .filter(col("length")>100)
   .orderBy("product_description")
  selecteddf = selecteddf.withColumn("product_size", when(col("length")<4000 && col("width") > 60, "Small")
    .when(col("length")<6000, "Medium")
    .when(col("length")<8000, "large")
    .otherwise("Extra Large")
  )
//window functions :: rank based on length for each category
  val windowSpec= Window.partitionBy("product_category"). orderBy(col("length").desc)
  val rankeddf = selecteddf.withColumn("Ranking", rank().over(windowSpec))
//  rankeddf.show(20)
//pivot
  val resdf = rankeddf.groupBy("product_name").pivot("product_category").agg(functions.min(col("length")))
  resdf.show()
//  val splitDf = productdfWithOutliers.withColumn("store_name", functions.split(col("product_number"), "_").getItem(0))
//    .withColumn("new_product_number", functions.split(col("product_number"), "_").getItem(1))
//  splitDf.show()
}
