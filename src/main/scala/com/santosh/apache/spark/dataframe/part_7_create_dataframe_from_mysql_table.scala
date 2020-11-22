package com.santosh.apache.spark.dataframe

import org.apache.spark.sql.SparkSession

object part_7_create_dataframe_from_mysql_table {

  def main(args: Array[String]): Unit = {

    println("Application started .....")

    val spark = SparkSession.builder()
      .appName("Create Dataframe From SQL Table")
      .master(master = "local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val mysql_db_driver_class = "com.mysql.jdbc.Driver"
    val table_name ="user_detail"
    val host_name = "localhost"
    val port_no = "3306"
    val user_name = "santosh"
    val password = "Santosh@12345"
    val database_name = "spark_db"

    val mysql_select_query = "(select * from " + table_name + ") as users"

    println("Printing mysql_select_query:")
    println(mysql_select_query)

    val mysql_jdbc_url = "jdbc:mysql://" + host_name + ":" + port_no + "/" + database_name

    println("Printing jdbc url:")
    println(mysql_jdbc_url)

    val users_data_df = spark.read.format(source = "jdbc")
      .option("url", mysql_jdbc_url)
      .option("driver", mysql_db_driver_class)
      .option("dbtable", mysql_select_query)
      .option("user", user_name)
      .option("password", password)
      .load()

    users_data_df.show(numRows = 5, truncate = false)

    spark.stop()
    println("Application ended .....")
  }
}
