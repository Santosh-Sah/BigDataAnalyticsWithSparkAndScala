package com.santosh.apache.spark.dataframe

import org.apache.spark.sql.SparkSession

object part_8_create_dataframe_from_mongo_collection {

  def main(args: Array[String]): Unit = {

    println("Application started .....")

    val spark = SparkSession.builder()
      .appName(name = "Create Dataframe from MongoDB Collection")
      .master(master = "local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val mongodb_host_name = "localhost"
    val mongodb_port_no = "27017"
    val mongodb_user_name = "admin"
    val mongodb_password = "admin"
    val mongodb_database_name = "spark_db"
    val mongodb_collection_name = "user_detail"

    val spark_mongodb_output_uri = "mongodb://" + mongodb_user_name + ":" + mongodb_password + "@" + mongodb_host_name + ":" + mongodb_port_no + "/" + mongodb_database_name + "." + mongodb_collection_name

    println("Printing spark_mongodb_output_uri:")
    println(spark_mongodb_output_uri)

    val user_df = spark.read
      .format(source = "mongo")
      .option("uri", spark_mongodb_output_uri)
      .option("database", mongodb_database_name)
      .option("collection", mongodb_collection_name)
      .load()

    user_df.show(numRows = 5, truncate = false)
    user_df.printSchema()

    spark.stop()
    println("Application ended .....")
  }
}
