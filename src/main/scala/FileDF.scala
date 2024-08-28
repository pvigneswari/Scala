package org.itc.com
import org.apache.spark.sql.SparkSession
object FileDF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HDFS File Reader")
      .master("local[*]") // Or the appropriate master for your cluster
      .getOrCreate()

    // Full HDFS path including the scheme and hostname
    val hdfsPath = "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/user/ec2-user/UKUSJULHDFS/vigu/insurance/Emp.csv"

    // Reading the file from HDFS
    val df = spark.read
      .option("header", "true") // Assuming the file has a header
      .csv(hdfsPath)

    // Show the data
    df.show()

    spark.stop()
  }
}
