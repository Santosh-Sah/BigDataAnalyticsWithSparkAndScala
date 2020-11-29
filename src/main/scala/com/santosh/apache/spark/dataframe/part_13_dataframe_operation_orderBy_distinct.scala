package com.santosh.apache.spark.dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object part_13_dataframe_operation_orderBy_distinct {

  def main(args: Array[String]): Unit = {

    println("Application started")

    val spark = SparkSession
      .builder()
      .appName(name = "Dataframe Operation: OrderBy, Distinct")
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
    user_df.orderBy(sortCol = "user_id").show(numRows = 5, truncate = false)

    println("Example 2: ")
    user_df.orderBy(user_df("user_id")).show(numRows = 5, truncate = false)

    println("Example 3: ")
    user_df.orderBy(sortCol = "user_id", sortCols = "user_name").show(numRows = 5, truncate = false)

    println("Example 4: ")
    import spark.implicits._
    user_df.orderBy($"user_id".asc, col(colName = "user_name").desc).show(numRows = 5, truncate = false)

    println("Example 5: ")
    user_df.select(col = "user_city").distinct().show(numRows = 5, truncate = false)

    println("Example 6: ")
    user_df.select(cols = $"user_city").distinct().show(numRows = 5, truncate = false)

    
    spark.stop()
    println("Application started")

  }
}
