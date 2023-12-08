# Databricks notebook source
dbutils.widgets.text("x","xxxx")
print(dbutils.widgets.get("x"))

# COMMAND ----------

connector_config = '''
{
  "src_connector_class_mappings" : {
    "deltalake": {
      "class_name": "deltalake"
      },
     "api": {
      "class_name": "GetData"
      } 
  },
  "dq_connector_class_mappings":{
  "spark": {
      "class_name": "spark_dq"
      }
  }

}
'''

# COMMAND ----------

processing_config = '''
{
  "temp_log_file_path": "/tmp/",
  "ingestion" : {
    "landing": {
      "read_options" : "{'header':'true','delimiter':',','inferSchema':'true'}",
      "file_format": "csv",
      "path": "dbfs:/FileStore/shared_uploads/landing_zone/"
    },
    "bronze": {
      "format": "csv",
      "path" : "dbfs:/FileStore/shared_uploads/bronze/",
      "write_command" : "insert overwrite "
      },
    "silver": {
      "db" : "silver",
      "table_format": "delta"
    },
    "gold": {
      "db" : "gold",
      "table_format": "delta"
    },
    "bronze_silver_mapping":
    {
      
      "Electricity.csv": 
      {
        "table_name": "electricity_consumption"
      },
      "House_Holds_And_Slums.csv":{
        "table_name":"House_Holds_Slums"
      },
      "Water.csv":{
        "table_name":"Water_Consumption"
      },
      "Crime_Data_from_2020_to_Present.csv":{
        "table_name":"Crime_Data_from_2020"
      }
    }
  }

}
'''