package org.itc.com

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object LoadDataToHive {

  def main(args: Array[String]): Unit = {
    // Check for the correct number of arguments
    if (args.length < 1) {
      println("Usage: LoadDataToHive <patient_file_path>")
      System.exit(1)
    }

    // Read file paths from command-line arguments
    val patientFilePath = args(0)
    // Uncomment and add paths if adding additional files
    // val hospitalTreatmentFilePath = args(1)
    // val insuranceFilePath = args(2)
    // val medicalCostFilePath = args(3)

    // Set the logger level to warn to reduce verbosity
    Logger.getLogger("org").setLevel(Level.WARN)

    // Initialize Spark Session with Hive support
    val spark = SparkSession.builder()
      .appName("Load CSV Data to Hive Tables")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    try {
      // Set the Hive database context to 'medicalcost'
      spark.sql("CREATE DATABASE IF NOT EXISTS medicalcost")
      spark.sql("USE medicalcost")

      // Define schema for the patient CSV file
      val patientSchema = StructType(Seq(
        StructField("patient_id", StringType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("address", StringType, nullable = true),
        StructField("phone", StringType, nullable = true),
        StructField("email", StringType, nullable = true)
      ))

      // Read patient CSV file into DataFrame with specified schema
      var patientDF = loadCsvToDataFrame(spark, patientFilePath, patientSchema)

      // Perform data cleaning and transformation
      patientDF = cleanPatientData(patientDF)

      // Write cleaned DataFrame to Hive table in 'medicalcost' database
      writeDataFrameToHive(patientDF, "patient")

      patientDF.printSchema()
      patientDF.show()

    } catch {
      case e: Exception =>
        println(s"Error processing files: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // Stop Spark Session
      spark.stop()
    }
  }

  def loadCsvToDataFrame(spark: SparkSession, filePath: String, schema: StructType): DataFrame = {
    spark.read
      .option("header", "true")
      .schema(schema)
      .csv(filePath)
  }

  def cleanPatientData(df: DataFrame): DataFrame = {
    df.withColumn("phone", regexp_replace(col("phone"), "[^0-9]", "")) // Remove non-numeric characters from phone
      .withColumn("email", lower(trim(col("email")))) // Standardize email to lowercase
      .na.fill("Unknown", Seq("name", "address", "email")) // Fill nulls with 'Unknown' for non-critical columns
  }

  def writeDataFrameToHive(df: DataFrame, tableName: String): Unit = {
    // Write DataFrame to a table in the 'medicalcost' Hive database
    df.write.mode("overwrite").saveAsTable(s"medicalcost.$tableName")
    println(s"Data inserted into Hive table: medicalcost.$tableName")
  }
}
