package com.santosh.apache.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object part_10_create_dataframe_from_kafka_topic {

  def main(args: Array[String]): Unit = {

    println("Application started")

    val spark = SparkSession.builder()
      .appName(name = "Create Dataframe From Kafka Topic")
      .master(master = "local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val kafka_topic_name = "userdetail"
    val kafka_bootstrap_servers = "localhost:9092"

    val user_df = spark.read.format(source = "kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
      .option("subscribe", kafka_topic_name)
      .load()

    val user_df_1 = user_df.selectExpr(exprs = "CAST(val as STRING)", "CAST(timestamp as TIMESTAMP")

    val user_schema = StructType(Array(
      StructField("user_id", IntegerType, true),
      StructField("user_name", StringType, true),
      StructField("user_city", StringType, true)
    ))

    val user_df_2 = user_df_1.select(from_json(col(colName = "value"), user_schema).as(alias = "user_detail"), col(colName = "timestamp"))

    val user_df_3 = user_df_2.select(col = "user_detail.*", cols = "timestamp")

    user_df_3.printSchema()
    user_df_3.show(numRows = 5, truncate = false)

    spark.stop()
    println("Application ended")
  }
}
