package com.santosh.apache.spark.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.databricks.spark.xml._
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

object part_6_create_column_from_nested_xml_file {

  def expand_nested_column(xml_data_df_temp: DataFrame): DataFrame = {
    var xml_data_df: DataFrame = xml_data_df_temp
    var select_clause_list = List.empty[String]

    for (column_name <- xml_data_df.schema.fieldNames) {

      println("Outside isinstance of loop: " + column_name)

      //checking column type is ArrayType
      if(xml_data_df.schema(column_name).dataType.isInstanceOf[ArrayType]) {

        println("inside isinstance ArrayType of loop: " + column_name)

        //extracted nested json cloumn/data using explode function
        xml_data_df = xml_data_df.withColumn(column_name, explode(xml_data_df(column_name)).alias(column_name))
        select_clause_list :+= column_name
      }

      //checking column type is StructType
      else if(xml_data_df.schema(column_name).dataType.isInstanceOf[StructType]) {

        println("inside isinstance StructType of loop: " + column_name)

        for (field <- xml_data_df.schema(column_name).dataType.asInstanceOf[StructType].fields) {

          select_clause_list :+= column_name + "." + field.name
        }
      }

      else {

        select_clause_list :+= column_name
      }
    }

    val columnNames = select_clause_list.map(name => col(name).alias(name.replace('.','_')))

    //selecting columns using select_clause_list from dataframe: xml_data_df
    xml_data_df.select(columnNames:_*)

  }

  def main(args: Array[String]): Unit = {

    println("Application started .....")

    val spark = SparkSession.builder()
      .appName(name = "Create Dataframe From XML File")
      .master(master = "local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val xml_file_path = "data/curriculam_complex_xml.xml"
    var xml_data_df = spark.read.option("rowTag","CourseOffering").xml(xml_file_path)

    xml_data_df.show(numRows = 5, truncate = false)
    xml_data_df.printSchema()

    //process the nested structure
    var nested_column_count = 1

    //run the while loop until the nested_column_count is zero(0)
    while (nested_column_count != 0) {
      println("Printing nested_column_count: " + nested_column_count)

      var nested_column_count_temp = 0

      //iterating each columns again to check if any next xml data is exists
      for (column_name <- xml_data_df.schema.fieldNames) {

        println("Iterating Dataframe columns: " + column_name)

        //checking column types is ArrayType
        if (xml_data_df.schema(column_name).dataType.isInstanceOf[ArrayType]
          || xml_data_df.schema(column_name).dataType.isInstanceOf[StructType]) {

          nested_column_count_temp += 1
        }
      }
      if (nested_column_count_temp != 0) {

        xml_data_df = expand_nested_column(xml_data_df)
        xml_data_df.show(numRows = 5, truncate = false)
      }

      println("nested_column_count_temp: " + nested_column_count_temp)
      nested_column_count = nested_column_count_temp

    }
  }
}
