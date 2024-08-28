package org.itc.com
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lower, trim}
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField, StructType}

object DFDemo extends App{
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
    StructField("length", DoubleType,nullable = false),
    StructField("width", DoubleType,nullable = false),
    StructField("height", FloatType,nullable = false)
  ))

  //explicit schema way2 - ddl string

  val ddlschema=
  """product_number string, product_name string,product_category string, product_scale string,
      | product_manufacturer string, product_description string, length double, width double, height float""".stripMargin


//  val productdf = spark.read.option("header",true).option("inferSchema",true).csv(path = "C:\\Users\\vigup\\Training\\Scala\\warehouse\\products.csv")
val productdf = spark.read.option("header",true).schema(ddlschema).csv(path = "C:\\Users\\vigup\\Training\\Scala\\warehouse\\clean\\productscleaned1.csv")
//  productdf.withColumn( colName = "length",col("length").cast("Float"))
//  change datatype from double to float
 var casteddf = productdf.withColumn( colName = "length",col("length").cast("Float"))
//  remove duplicates
   casteddf=casteddf.dropDuplicates("product_number")
//  missing values
  var cleaneddf = casteddf.na.fill(value ="unknown",Seq("product_name")).na.fill(value =0,Seq("length","width","height"))
  //trim spaces in begin and end
    cleaneddf = cleaneddf.withColumn(colName="product_name",trim(col(colName="product_name")))
// change case
  cleaneddf = cleaneddf.withColumn(colName="product_category",lower(col(colName="product_category")))
    .withColumn("product_name",lower(col("product_name")))
  //  filter valid data
  var validdf = cleaneddf.filter(col("width")>85)
//  drop columns, column pruning
  validdf = validdf.drop("product_scale")
//outlier ---task
  validdf.show(5)
  validdf.coalesce(1).write.csv("C:\\Users\\vigup\\Training\\Scala\\warehouse\\clean\\productscleaned2")
  //  productdf.printSchema()
//  productdf.show( numRows = 5)
//  cleaneddf.show(5)
//  casteddf.printSchema()
//  casteddf.show(numRows =5)
}
