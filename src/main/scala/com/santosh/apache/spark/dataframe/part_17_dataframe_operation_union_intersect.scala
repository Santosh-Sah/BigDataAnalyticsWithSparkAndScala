package com.santosh.apache.spark.dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object part_17_dataframe_operation_union_intersect {

  def main(args: Array[String]): Unit = {

    println("Application started")

    val spark = SparkSession
      .builder()
      .appName(name = "Dataframe Operation: Union, Intersect, Except")
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

    user_df.show(numRows = 10, truncate = false)
    user_df.printSchema()

    val user_seq_2 = Seq(
      Row(6, "Manoj", "Lar Road")
    )

    val user_df_1 = spark.createDataFrame(spark.sparkContext.parallelize(user_seq_2), user_schema)

    user_df_1.show(numRows = 10, truncate = false)
    user_df_1.printSchema()

    println("Example 1: ")
    user_df.union(user_df_1).select(col = "*").show(numRows = 10, truncate = false)

    println("Example 2: ")
    user_df.intersect(user_df_1).select(col = "*").show(numRows = 10, truncate = false)

    println("Example 3: ")
    user_df.except(user_df_1).select(col = "*").show(numRows = 10, truncate = false)

    spark.stop()
    println("Application started")
  }
}
