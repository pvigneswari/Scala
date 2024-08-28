package org.itc.com
import org.apache.spark.sql.{SparkSession, DataFrame}
object MedicalDataAnalysis {

  def main(args: Array[String]): Unit = {
    // Check for correct number of arguments
    if (args.length < 2) {
      println("Usage: MedicalDataAnalysis <patient_file_path> <hospital_file_path>")
      System.exit(1)
    }
    val patientFilePath = args(0)
    val hospitalFilePath = args(1)
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Medical Data Analysis")
      .config("spark.master", "local")
      .getOrCreate()
    // Read Patient Details CSV
    val patientDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(patientFilePath)
    // Read Hospital Treatment CSV
    val treatmentDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(hospitalFilePath)
    // Display the schema of both DataFrames
    patientDF.printSchema()
    treatmentDF.printSchema()
    // Join DataFrames on 'patient_id'
    val joinedDF = patientDF.join(treatmentDF, "patient_id")
    // Display the joined DataFrame
    joinedDF.show(5)
    // Perform analysis, e.g., total treatment cost per patient
    val totalCostPerPatient = joinedDF.groupBy("patient_id", "name")
      .sum("treatment_cost")
      .withColumnRenamed("sum(treatment_cost)", "total_treatment_cost")

    totalCostPerPatient.show()

    // Stop Spark Session
    spark.stop()
  }
}
