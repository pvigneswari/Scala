//package org.itc.com
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.functions.{col, lower, trim}
//import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField, StructType}
//import org.apache.spark.sql.functions._
//object CustomerSpending extends App {
//{
//  // Set up the Spark configuration and session
//  val sparkConf = new SparkConf()
//    .setAppName("CustomerSpending")
//    .setMaster("local[*]") // Adjust as needed
//
//  val spark = SparkSession.builder()
//    .config(sparkConf)
//    .getOrCreate()
//
//  // JDBC connection properties
//  val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
//  val connectionProperties = new java.util.Properties()
//  connectionProperties.setProperty("user", "postgres")
//  connectionProperties.setProperty("password", "August@12345678")
//  connectionProperties.setProperty("driver", "org.postgresql.Driver")
//
//  // Load the sales table
//  val salesDF: DataFrame = spark.read
//    .jdbc(jdbcUrl, "sales", connectionProperties)
//
//  // Load the menu table
//  val menuDF: DataFrame = spark.read
//    .jdbc(jdbcUrl, "menu", connectionProperties)
//
//  // Load the members table (optional, if you need member data)
//  val membersDF: DataFrame = spark.read
//    .jdbc(jdbcUrl, "members", connectionProperties)
//
//  // Show the data (for debugging purposes)
//  salesDF.show()
//  menuDF.show()
//  // Perform the join between sales and menu tables
//  val joinedDF = salesDF.join(menuDF, "product_id")
//
//  // Calculate total amount spent by each customer
//  val totalSpentDF = joinedDF
//    .groupBy("customer_id")
//    .agg(sum(col("price")).as("total_spent"))
//
//  // Show the result
//  totalSpentDF.show()
//
//}
//
//}



package org.itc.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object CustomerSpending extends App {
  // Set up the Spark configuration and session
  val sparkConf = new SparkConf()
    .setAppName("CustomerSpending")
    .setMaster("local[*]") // Adjust as needed

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
  // Import implicits
  import spark.implicits._

  // JDBC connection properties
  val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
  val connectionProperties = new java.util.Properties()
  connectionProperties.setProperty("user", "postgres")
  connectionProperties.setProperty("password", "August@12345678")
  connectionProperties.setProperty("driver", "org.postgresql.Driver")

  // Load the tables
  val salesDF: DataFrame = spark.read.jdbc(jdbcUrl, "sales", connectionProperties)
  val menuDF: DataFrame = spark.read.jdbc(jdbcUrl, "menu", connectionProperties)
  val membersDF: DataFrame = spark.read.jdbc(jdbcUrl, "members", connectionProperties)

  // Query 1: Calculate total amount spent by each customer
  val totalSpentDF = salesDF.join(menuDF, "product_id")
    .groupBy("customer_id")
    .agg(sum("price").alias("total_spent"))
  totalSpentDF.show()

//  // Query 2: How many days has each customer visited the restaurant?
val visitDaysDF = salesDF
  .select("customer_id", "order_date")
  .distinct()
  .groupBy("customer_id")
  .agg(countDistinct("order_date").alias("visit_days"))
  visitDaysDF.show()
//  // Query 3: What was the first item from the menu purchased by each customer?
val firstItemDF = salesDF.join(menuDF, "product_id")
  .withColumn("rank", row_number().over(Window.partitionBy("customer_id").orderBy("order_date")))
  .filter($"rank" === 1)
  .select("customer_id", "product_name")
  firstItemDF.show()

//  // Query 4: What is the most purchased item on the menu and how many times was it purchased by all customers?
  val mostPurchasedItemDF = salesDF
    .groupBy("product_id")
    .agg(count("product_id").as("purchase_count"))
    .join(menuDF, "product_id")
    .orderBy(desc("purchase_count"))
    .limit(1)
  mostPurchasedItemDF.show()

//  // Query 5: Which item was the most popular for each customer?
val mostPopularItemDF = salesDF.join(menuDF, "product_id")
  .groupBy("customer_id", "product_name")
  .agg(count("product_id").alias("purchase_count"))
  .withColumn("rank", row_number().over(Window.partitionBy("customer_id").orderBy(desc("purchase_count"))))
  .filter($"rank" === 1)
  .select("customer_id", "product_name")
  mostPopularItemDF.show()

  // Register DataFrames as temporary views for SQL queries
  salesDF.createOrReplaceTempView("sales")
  menuDF.createOrReplaceTempView("menu")
  membersDF.createOrReplaceTempView("members")

//  // Query 6: Which item was purchased first by the customer after they became a member?
  val firstPurchaseAfterMembershipDF = spark.sql("""
    SELECT s.customer_id, s.product_id, m.product_name, MIN(s.order_date) AS first_purchase_date
    FROM sales s
    JOIN menu m ON s.product_id = m.product_id
    JOIN members mb ON s.customer_id = mb.customer_id
    WHERE s.order_date >= mb.join_date
    GROUP BY s.customer_id, s.product_id, m.product_name
  """)
  firstPurchaseAfterMembershipDF.show()

//  // Query 7: Which item was purchased just before the customer became a member?
  val purchaseBeforeMembershipDF = spark.sql("""
    SELECT s.customer_id, s.product_id, m.product_name, MAX(s.order_date) AS last_purchase_before_membership
    FROM sales s
    JOIN menu m ON s.product_id = m.product_id
    JOIN members mb ON s.customer_id = mb.customer_id
    WHERE s.order_date < mb.join_date
    GROUP BY s.customer_id, s.product_id, m.product_name
  """)
  purchaseBeforeMembershipDF.show()

//  // Query 8: What is the total items and amount spent for each member before they became a member?
  val totalSpentBeforeMembershipDF = spark.sql("""
    SELECT s.customer_id, COUNT(s.product_id) AS total_items, SUM(m.price) AS total_spent
    FROM sales s
    JOIN menu m ON s.product_id = m.product_id
    JOIN members mb ON s.customer_id = mb.customer_id
    WHERE s.order_date < mb.join_date
    GROUP BY s.customer_id
  """)
  totalSpentBeforeMembershipDF.show()

//  // Query 9: How many points would each customer have?
  val pointsDF = spark.sql("""
    SELECT s.customer_id,
           SUM(
             CASE
               WHEN m.product_name = 'sushi' THEN m.price * 20
               ELSE m.price * 10
             END
           ) AS total_points
    FROM sales s
    JOIN menu m ON s.product_id = m.product_id
    GROUP BY s.customer_id
  """)
  pointsDF.show()

////  // Query 10: How many points do customer A and B have at the end of January?
  spark.sql("""
    SELECT s.customer_id,
      SUM(CASE
        WHEN s.order_date BETWEEN ms.join_date AND ms.join_date + INTERVAL '7' DAY THEN m.price * 20
        WHEN m.product_name = 'sushi' THEN m.price * 20
        ELSE m.price * 10
      END) as total_points
    FROM sales s
    JOIN menu m ON s.product_id = m.product_id
    JOIN members ms ON s.customer_id = ms.customer_id
    WHERE s.order_date <= '2024-01-31'
    GROUP BY s.customer_id
    """).show()

  // Stop the Spark session
  spark.stop()
}

