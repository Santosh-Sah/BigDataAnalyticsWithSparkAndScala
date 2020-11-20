package com.santosh.apache.spark.dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

case class User(user_id: Int, user_name: String, user_city: String)

object create_first_apache_spark_dataframe_part_1 {

  def main(args: Array[String]): Unit = {

    println("Application started")

    val spark = SparkSession
                .builder()
                .appName(name = "Create First Apache Spark Dataframe")
                .master(master = "local[*]")
                .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("Approach 1:")

    val user_list = List(
      (1, "Santosh", "Gurgaon"),
      (2, "Sunil", "Siwan"),
      (3, "Yogyata", "New Delhi"),
      (4, "Indoo", "Deoria"),
      (5, "Jeni", "Mairwa")
    )

    val df_columns = Seq("user_id", "user_name", "user_city")
    val user_rdd = spark.sparkContext.parallelize(user_list)
    val user_df = spark.createDataFrame(user_rdd)
    user_df.show(numRows = 5, truncate = false)

    println(user_df.getClass())

    val users_df_1 = user_df.toDF(df_columns:_*)
    println(users_df_1.getClass())
    users_df_1.show(numRows = 5, truncate = false)

    println("Approach 2:")

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

    val user_df_2 = spark.createDataFrame(spark.sparkContext.parallelize(user_seq), user_schema)

    user_df_2.show(numRows = 5, truncate = false)

    println("Approach 3:")

    val case_user_seq = Seq(
      User(1, "Santosh", "Gurgaon"),
      User(2, "Sunil", "Siwan"),
      User(3, "Yogyata", "New Delhi"),
      User(4, "Indoo", "Deoria"),
      User(5, "Jeni", "Mairwa")
    )

    val case_users_rdd = spark.sparkContext.parallelize(case_user_seq)
    val case_user_df = spark.createDataFrame(case_users_rdd)

    case_user_df.show(numRows = 5, truncate = false)

    spark.stop()
    println("Application Completed")
  }
}