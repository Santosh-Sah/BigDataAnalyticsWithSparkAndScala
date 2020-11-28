package com.santosh.apache.spark.dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object part_15_dataframe_operation_join_inner_cross {

  def main(args: Array[String]): Unit = {

    println("Application started")

    val spark = SparkSession
      .builder()
      .appName(name = "Create First Apache Spark Dataframe")
      .master(master = "local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val user_seq = Seq(
      Row(1, "Santosh", "Gurgaon"),
      Row(2, "Sunil", "Siwan"),
      Row(3, "Yogyata", "New Delhi"),
      Row(4, "Indoo", "Deoria"),
      Row(5, "Jeni", "Mairwa")
    )

    val user_schema = StructType(Array(
      StructField("user_id", IntegerType, true),
      StructField("user_name", StringType, true),
      StructField("user_city", StringType, true)
    ))

    val user_df = spark.createDataFrame(spark.sparkContext.parallelize(user_seq), user_schema)

    user_df.show(numRows = 5, truncate = false)
    user_df.printSchema()

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

    order_df.show(numRows = 10, truncate = false)
    order_df.printSchema()

    val inner_join_df_1 = order_df.join(user_df, Seq("user_id"), joinType = "inner")
      .select(col = "user_id", cols = "order_id", "card_type", "product_category", "order_amount", "order_datetime", "user_name", "user_city")

    inner_join_df_1.show(numRows = 10, truncate = false)

    import spark.implicits._
    val inner_join_df_2 = order_df.as(alias = "o").join(user_df.as(alias = "u"), joinExprs = ($"o.user_id" === $"u.user_id"), joinType = "inner")
    inner_join_df_2.show(numRows = 10, truncate = false)

    val cross_join_df_1 = user_df.crossJoin(user_df)
    cross_join_df_1.show(numRows = 10, truncate = false)

    val cross_join_df_2 = user_df.as(alias = "u1").join(user_df.as(alias = "u2"), joinExprs = ($"u1.user_id" === $"u2.user_id"), joinType = "cross")
    cross_join_df_2.show(numRows = 100, truncate = false)

    spark.stop()
    println("Application ended")

  }
}
