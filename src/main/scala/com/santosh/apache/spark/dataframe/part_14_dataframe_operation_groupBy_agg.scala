package com.santosh.apache.spark.dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, DoubleType}
import org.apache.spark.sql.functions._

object part_14_dataframe_operation_groupBy_agg {

  def main(args: Array[String]): Unit = {

    println("Application started")

    val spark = SparkSession
      .builder()
      .appName(name = "Dataframe Operation: GroupBy, Aggregation")
      .master(master = "local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val order_seq = Seq(Row(1, "Visa", "Appliances", 112.15, "2017-05-31 08:30:45", 3),
      Row(2, "Maestro", "Electronics", 1156.18, "2017-07-28 11:30:20", 1),
      Row(3, "Visa", "Computer & Accessories", 142.67, "2018-03-25 17:45:15", 6),
      Row(4, "MasterCard", "Electronics", 817.15, "2018-06-12 06:30:35", 5),
      Row(5, "Maestro", "Garden & Outdoors", 54.17, "2018-11-05 22:30:45", 1),
      Row(6, "Visa", "Electronics", 112.15, "2019-01-26 20:30:45", 2),
      Row(7, "MasterCard", "Appliances", 4562.37, "2019-02-18 08:56:45", 5),
      Row(8, "Visa", "Computer & Accessories", 3500.65, "2019-07-24 09:33:45", 8),
      Row(9, "MasterCard", "Books", 200.05, "2019-09-22 08:40:55", 10),
      Row(10, "MasterCard", "Electronics", 563.15, "2019-11-19 19:40:15", 3))

    val order_schema = StructType(Array(
      StructField("order_id", IntegerType, true),
      StructField("card_type", StringType, true),
      StructField("product_category", StringType, true),
      StructField("order_amount", DoubleType, true),
      StructField("order_datetime", StringType, true),
      StructField("user_id", IntegerType, true)
    ))

    val order_df = spark.createDataFrame(spark.sparkContext.parallelize(order_seq), order_schema)

    order_df.show(numRows = 5, truncate = false)
    order_df.printSchema()

    println("Example 1: ")
    val order_group_1 = order_df.groupBy(col1 = "card_type")
    println("Type orders_group_1: ")
    println(order_group_1.getClass)
    println(order_group_1.toString())

    println("Example 2: ")
    val order_group_2 = order_df.groupBy(col1 = "card_type", cols = "product_category")
    println("Type orders_group_2: ")
    println(order_group_2.getClass)
    println(order_group_2.toString())

    println("Example 3: ")
    order_df.select(col = "card_type").distinct().show(numRows = 10, truncate = false)

    println("Example 4: ")
    order_df.groupBy(col1 = "card_type").agg(count(columnName = "order_id")).show(numRows = 10, truncate = false)

    println("Example 5: ")
    order_df.groupBy(col1 = "card_type").agg(count(columnName = "order_id").alias(alias = "order_count")).show(numRows = 10, truncate = false)

    println("Example 6: ")
    order_df.groupBy(col1 = "card_type", cols = "product_category").agg(sum(columnName = "order_amount")).show(numRows = 10, truncate = false)

    println("Example 7: ")
    order_df.groupBy(col1 = "card_type", cols = "product_category").agg(sum(columnName = "order_amount").alias(alias = "total_order_amount")).show(numRows = 10, truncate = false)


    spark.stop()
    println("Application started")
  }
}
