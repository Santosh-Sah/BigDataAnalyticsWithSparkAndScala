package com.santosh.apache.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object part_3_create_dataframe_from_json_file {

  def main(args: Array[String]): Unit = {
    println("Application started ......")

    println("Approach 1 started ......")

    val spark = SparkSession
      .builder()
      .appName("Create Dataframe From JSON File")
      .master(master = "local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val json_file_path = "data/user_detail.json"
    val user_df_1 = spark.read.json(json_file_path)
    user_df_1.show(numRows = 5, truncate = false)
    user_df_1.printSchema()

    println("Approach 1 ended ......")

    println("Approach 2 started ......")

    val json_multiline_file_path = "data/user_detail_multiline.json"
    val user_df_2 = spark.read
      .option("multiline", true)
      .json(json_multiline_file_path)

    user_df_2.show(numRows = 5, truncate = false)
    user_df_2.printSchema()

    println("Approach 2 ended ......")

    println("Approach 3 started ......")

    val json_multiline_in_list_file_path = "data/user_detail_multiline_in_list.json"

    val user_schema = StructType(Array(
      StructField("user_id", IntegerType, true),
      StructField("user_name", StringType, true),
      StructField("user_city", StringType, true)
    ))

    val user_df_3 = spark.read
      .option("multiline",true)
      .schema(user_schema)
      .json(json_multiline_in_list_file_path)

    user_df_3.show(numRows = 5, truncate = false)
    user_df_3.printSchema()

    println("Approach 3 ended ......")

    spark.stop()
    println("Application completed")
  }
}
