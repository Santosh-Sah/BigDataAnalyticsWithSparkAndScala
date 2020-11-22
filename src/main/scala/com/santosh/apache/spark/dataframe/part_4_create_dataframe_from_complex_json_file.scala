package com.santosh.apache.spark.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col

object part_4_create_dataframe_from_complex_json_file {

  def main(args: Array[String]): Unit = {

    println("Application started .....")

    val spark = SparkSession.builder()
      .appName(name = "Create Dataframe From Complex JSON File")
      .master(master = "local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val json_file_path = "data/complex_json.json"
    val json_data_df = spark.read
      .option("multiline", true)
      .json(json_file_path)

    json_data_df.show(numRows = 5, truncate = false)
    json_data_df.printSchema()

    //process the complex structure
    var nested_column_count = 1

    //run the while loop till the nested_column_count is zero
    while(nested_column_count != 0) {

      println("Printing nested_column_count: " + nested_column_count)

      var nested_column_count_temp = 0

      //iterating each columns again to check if any next xml data is exist
      for (column_name <- json_data_df.schema.fields){

        println("Iterating dataframe columns: " + column_name)

        //checking column type is ArrayType
        if(column_name.dataType.isInstanceOf[ArrayType]){
          //here the column is ArrayType
          nested_column_count_temp += 1
        }

        else if(column_name.dataType.isInstanceOf[StructType]) {
          //here the column is StructType
          nested_column_count_temp += 1
        }

      }
    }
    println("Application stopped .....")
  }
}
