package com.santosh.apache.spark.dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

case class User(user_id: Int, user_name: String, user_city: String)

object create_first_apache_spark_dataframe_part_1 {

  def main(args: Array[String]): Unit = {

    println("Application started")
  }
}