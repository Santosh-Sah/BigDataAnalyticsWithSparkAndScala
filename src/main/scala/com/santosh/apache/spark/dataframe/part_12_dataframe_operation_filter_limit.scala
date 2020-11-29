package com.santosh.apache.spark.dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object part_12_dataframe_operation_filter_limit {

  def main(args: Array[String]): Unit = {

    println("Application started")

    val spark = SparkSession
      .builder()
      .appName(name = "Dataframe Operation: filter")
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
    user_df.filter(conditionExpr = "user_id == 4").show(numRows = 5, truncate = false)

    println("Example 2: ")
    user_df.filter(conditionExpr = "user_id == 4 or user_id == 3").show(numRows = 5, truncate = false)

    println("Example 3: ")
    user_df.filter(conditionExpr = "user_id == 4 or user_city == 'Gurgaon'").show(numRows = 5, truncate = false)

    println("Example 4: ")
    user_df.filter(conditionExpr = "user_name like '%s'").show(numRows = 5, truncate = false)

    println("Example 5: ")
    user_df.filter(conditionExpr = "user_id > 3").show(numRows = 5, truncate = false)

    println("Example 6: ")
    user_df.filter(user_df.col(colName = "user_id") < 3).show(numRows = 5, truncate = false)

    println("Example 7: ")
    user_df.filter(user_df.col(colName = "user_city") === "Gurgaon").show(numRows = 5, truncate = false)

    spark.stop()
    println("Application ended")
  }
}
