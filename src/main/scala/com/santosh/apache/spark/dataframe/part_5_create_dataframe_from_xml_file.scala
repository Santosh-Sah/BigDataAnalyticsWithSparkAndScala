package com.santosh.apache.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import com.databricks.spark.xml._

object part_5_create_dataframe_from_xml_file {
  def main(args: Array[String]): Unit = {
    println("Application started .....")

    val spark = SparkSession.builder()
      .appName(name = "Create Dataframe From XML File")
      .master(master = "local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("Approach 1 started .....")

    val xml_file_path = "data/user_detail.xml"
    val user_df_1 = spark.read.option("rowTag","user").xml(xml_file_path)

    user_df_1.show(numRows = 5, false)
    user_df_1.printSchema()

    println("Approach 1 ended .....")

    println("Approach 2 started .....")

    val user_schema = StructType(Array(

      StructField("user_id", IntegerType, true),
      StructField("user_name", StringType, true),
      StructField("user_city", StringType, true)
    ))

    val user_df_2 = spark.read.schema(user_schema).option("rowTag","user").xml(xml_file_path)

    user_df_2.show(numRows = 5, truncate = false)
    user_df_2.printSchema()

    println("Approach 2 ended .....")

    spark.stop()
    println("Application ended .....")
  }
}
