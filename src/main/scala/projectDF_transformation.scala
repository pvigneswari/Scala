//package org.itc.com
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.{SparkSession, functions}
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.functions.{col, count, countDistinct, desc, lower, rank, row_number, sum, trim, when}
//import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField, StructType}
//import org.itc.com.CustomerSpending.{menuDF, salesDF}
//import org.itc.com.DFDemo_transformation.{productschema, spark}
//
//object projectDF_transformation extends App {
//
//  Logger.getLogger( "org").setLevel(Level.ERROR)
//
//  val sparkConf =new SparkConf()
//  sparkConf.set("spark.app.name","DataframeDemo")
//  sparkConf.set("spark.master","local[*]")
//  //val ddlschema = """product_number"""
//  val spark = SparkSession.builder().config(sparkConf).getOrCreate();
//  //explicit schema way1
//  val patientschema = StructType(Array(
//    StructField("patient_id",StringType,nullable = false),
//    StructField("name",StringType,nullable = false),
//    StructField("address",StringType,nullable = false),
//    StructField("phone",StringType,nullable = false),
//    StructField("email",StringType,nullable = false)
//    ))
//  val hospitalschema=
//    """product_number string, product_name string,product_category string, product_scale string,
//      | product_manufacturer string, product_description string, length double, width double, height float""".stripMargin
//  val insuranceschema = """order_number string,product_number string,product_category string,price int, quantity int"""
//  val medicalCostschema = """order_number string,product_number string,product_category string,price int, quantity int"""
//
//  val patientdf = spark.read.option("header", true).schema(patientschema).csv("C:\\Users\\vigup\\IdeaProjects\\SparkDemoJul\\output\\outputDir\\patient\\")
//  val selecteddf = patientdf.select("patient_id","name")
//    .filter(col("patient_id")>"P0050")
//    .orderBy("name")
//  selecteddf.show()
//
//
//  // Query 1: Calculate total amount spent by each customer
//  val totalSpentDF = salesDF.join(menuDF, "product_id")
//    .groupBy("customer_id")
//    .agg(sum("price").alias("total_spent"))
//  totalSpentDF.show()
//
//  //  // Query 2: How many days has each customer visited the restaurant?
//  val visitDaysDF = salesDF
//    .select("customer_id", "order_date")
//    .distinct()
//    .groupBy("customer_id")
//    .agg(countDistinct("order_date").alias("visit_days"))
//  visitDaysDF.show()
//  //  // Query 3: What was the first item from the menu purchased by each customer?
//  val firstItemDF = salesDF.join(menuDF, "product_id")
//    .withColumn("rank", row_number().over(Window.partitionBy("customer_id").orderBy("order_date")))
//    .filter($"rank" === 1)
//    .select("customer_id", "product_name")
//  firstItemDF.show()
//
//  //  // Query 4: What is the most purchased item on the menu and how many times was it purchased by all customers?
//  val mostPurchasedItemDF = salesDF
//    .groupBy("product_id")
//    .agg(count("product_id").as("purchase_count"))
//    .join(menuDF, "product_id")
//    .orderBy(desc("purchase_count"))
//    .limit(1)
//  mostPurchasedItemDF.show()
//
//}
//
