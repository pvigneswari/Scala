package org.itc.com
import org.apache.log4j.{Level, Logger}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lower, rank, trim, when}
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

object sparksqldemo extends App {

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
  productdf.cache() //if we are using productdf multiple times then it is good to use cache(cache memory is used when frequently access the data)
  productdf.persist(StorageLevel.MEMORY_AND_DISK_SER)

  productdf.createOrReplaceTempView("product")

  spark.sql("select * from product order by product_category").show(5)
}
