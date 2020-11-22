package com.santosh.apache.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object part_2_create_dataframe_from_csv_file {

  def main(args: Array[String]): Unit = {
    println("Application started ......")

    println("Approach 1 started ......")

    val spark = SparkSession
      .builder()
      .appName("Create Dataframe From CSV File")
      .master(master = "local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val csv_comma_delimited_file_path = "data/user_detail_comma_delimiter.csv"
    val user_df_1 = spark.read.option("header", true).csv(csv_comma_delimited_file_path)
    user_df_1.show(numRows = 5, truncate = false)
    user_df_1.printSchema()

    println("Approach 1 ended ......")

    println("Approach 2 started ......")

    val user_df_2 = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(csv_comma_delimited_file_path)

    val user_schema = StructType(Array(
      StructField("user_id", IntegerType, true),
      StructField("user_name", StringType, true),
      StructField("user_city", StringType, true)
    ))

    user_df_2.show(numRows = 5, truncate = false)
    user_df_2.printSchema()

    println("Approach 2 ended ......")

    println("Approach 3 started ......")

    val csv_pipe_delimited_file_path = "data/user_detail_pipe_delimiter.csv"
    val user_df_3 = spark.read
      .option("sep","|")
      .option("header", true)
      .schema(user_schema)
      .csv(csv_pipe_delimited_file_path)

    user_df_3.show(numRows = 5, truncate = false)
    user_df_3.printSchema()

    println("Approach 3 ended ......")

    spark.stop()
    println("Application completed")
  }
}
