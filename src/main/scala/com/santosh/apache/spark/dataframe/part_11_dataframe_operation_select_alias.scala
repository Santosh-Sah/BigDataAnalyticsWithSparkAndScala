package com.santosh.apache.spark.dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object part_11_dataframe_operation_select_alias {

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

    println("Example 1: ")
    user_df.select(col = "*").show(numRows = 5, truncate = false)

    println("Example 2: ")
    user_df.select(col = "user_id", cols = "user_name", "user_city").show(numRows = 5, truncate = false)

    println("Example 3: ")
    user_df.select(user_df("user_id"), user_df("user_name")).show(numRows = 5, truncate = false)

    println("Example 4: ")
    user_df.select(user_df.col(colName = "user_id"), user_df.col(colName = "user_city")).show(numRows = 5, truncate = false)

    println("Example 5: ")
    import spark.implicits._
    user_df.select(user_df.col(colName = "user_id").alias(alias = "userId"), $"user_name").show(numRows = 5, truncate = false)

    spark.stop()
    println("Application ended")
  }
}
