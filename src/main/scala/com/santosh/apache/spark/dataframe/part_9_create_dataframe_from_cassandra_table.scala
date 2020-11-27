package com.santosh.apache.spark.dataframe

import org.apache.spark.sql.SparkSession

object part_9_create_dataframe_from_cassandra_table {

  def main(args: Array[String]): Unit = {

    println("Application started")

    val spark = SparkSession.builder()
      .appName(name = "Create Dataframe From Cassandra Table")
      .master(master = "local[*]")
      .getOrCreate()

    val cassandra_host_name = "localhost"
    val cassandra_port_no = "9042"
    val cassandra_ks_name = "demo_ks"
    val cassandra_table_name = "user_detail"
    val cassandra_cluster_name = "test_cluster"

    val user_df = spark.read.format(source = "org.apache.spark.sql.cassandra")
      .options(Map("table" -> cassandra_table_name,
        "keyspace" -> cassandra_ks_name,
        "host" -> cassandra_host_name,
        "port" -> cassandra_port_no,
        "cluster" -> cassandra_cluster_name)).load()

    user_df.printSchema()
    user_df.show(numRows = 5, truncate = false)

    spark.stop()
    println("Application ended")
  }
}
