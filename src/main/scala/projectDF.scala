package org.itc.com

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.typesafe.config.ConfigFactory

object projectDF extends App {

  val config = ConfigFactory.load()

  //extracting database connection details from the config file
  val dbUrl = config.getString("database.url")
  val dbUser = config.getString("database.user")
  val dbPassword = config.getString("database.password")

  val patientDetailsTable = config.getString("database.tables.patientDetails")
  val hospitalTreatmentTable = config.getString("database.tables.hospitalTreatment")
  val insuranceTable = config.getString("database.tables.insurance")
  val medicalCostTable = config.getString("database.tables.medicalCost")

  // Initialize Spark session
  val spark = SparkSession.builder()
    .appName("MedicalCostCleaner")
    .master("local[*]")
    .getOrCreate()
  // Read the tables
//  val patientDetailsDF = {spark.read.format("jdbc").option("url",dbUrl).option("dbtable",patientDetailsTable).option("user",dbUser).option("password",dbPassword).load().cache()}
//  val hospitalTreatmentDF = spark.read.format("jdbc").option("url",dbUrl).option("dbtable",hospitalTreatmentTable).option("user",dbUser).option("password",dbPassword).load().cache()
//  val insuranceDF = spark.read.format("jdbc").option("url",dbUrl).option("dbtable",insuranceTable).option("user",dbUser).option("password",dbPassword).load().cache()
  val medicalCostDF = spark.read.format("jdbc").option("url",dbUrl).option("dbtable",medicalCostTable).option("user",dbUser).option("password",dbPassword).load().cache()

//  // Print schema to understand the data
  medicalCostDF.printSchema()
  medicalCostDF.show(5)
  // Clean the data: Example cleaning steps
val cleanedDF = medicalCostDF
  .na.drop("all")// Remove rows where all columns are null
  .filter(col("patient_id").isNotNull) // Ensure critical fields like "patient_id" are not null
  .withColumn( colName = "medical_cost",col("medical_cost").cast("Integer")) //change datatype to integer
  .withColumn("date_of_treatment", to_timestamp(col("date_of_treatment"), "yyyy-MM-dd")) // Convert time_published to Timestamp

  val outputDir = "C:\\Users\\vigup\\IdeaProjects\\SparkDemoJul\\output\\outputDir"
  medicalCostDF.write
    .format("csv")
    .option("header", "true")
    .mode("overwrite") // Options: "overwrite", "append", "ignore", "error"
    .save(s"$outputDir/medicalCost")
  // Print schema to understand the data
  cleanedDF.printSchema()
}
