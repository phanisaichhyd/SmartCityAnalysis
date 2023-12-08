# Databricks notebook source
import json
from datetime import datetime,timedelta

# COMMAND ----------

dbutils.widgets.text("source_type","deltalake")

# COMMAND ----------

source_type = dbutils.widgets.get("source_type")

# COMMAND ----------

# MAGIC %run ../config/etl_config

# COMMAND ----------

# MAGIC %run ../utils/utils

# COMMAND ----------

code_config = config.get_config(json_var = connector_config)
etl_params = config.get_config(json_var = processing_config)
obj_data_ops = data_ops(source_type,code_config)
obj_dl = obj_data_ops.get_source_obj()

# COMMAND ----------

landing_files = dbutils.fs.ls(etl_params["ingestion"]["landing"]["path"])
bronze_files = dbutils.fs.ls(etl_params["ingestion"]["bronze"]["path"])
bronze_silver_mappings = etl_params["ingestion"]["bronze_silver_mapping"]
default_silver_table_ops = etl_params["ingestion"]["bronze"]["write_command"]
silver_db = etl_params["ingestion"]["silver"]["db"]
silver_default_table_format = etl_params["ingestion"]["silver"]["table_format"]
gold_db = etl_params["ingestion"]["gold"]["db"]
gold_default_table_format = etl_params["ingestion"]["gold"]["table_format"]
#print(silver_db)

# COMMAND ----------

landing_path  = etl_params["ingestion"]["landing"]["path"]
landing_default_read_params  = etl_params["ingestion"]["landing"]["read_options"]
landing_default_file_format  = etl_params["ingestion"]["landing"]["file_format"]
bronze_path  = etl_params["ingestion"]["bronze"]["path"]
bronze_default_file_format  = etl_params["ingestion"]["bronze"]["format"]
print(landing_path,landing_default_read_params,landing_default_file_format,bronze_path,bronze_default_file_format)

# COMMAND ----------

#bronze_files = []
#landing_files = []
if len(bronze_files) >0 :
  bronze_files = [files.name for files in bronze_files]
print("Cleaning up Bronze Layer")
for f in bronze_files:
  dbutils.fs.rm(bronze_path + f)
if len(landing_files) > 0:
  landing_files = [files.name for files in landing_files]
print("Moving Landing files to bronze layer")
for f in landing_files:
  print("Moving " + f + " from landing to bronze")
  dbutils.fs.cp(landing_path + f ,bronze_path)
print("All files are moved from landing area to bronze")


# COMMAND ----------

data_to_ingest = dbutils.fs.ls(bronze_path)
for data_file_itr in data_to_ingest:
  data_file = data_file_itr.name
  print(data_file)

# COMMAND ----------

data_to_ingest = dbutils.fs.ls(bronze_path)
f1 = data_to_ingest[0]
f1.size/1024

# COMMAND ----------

s1 = datetime.now()
s2 = datetime.now()
print((s2-s1).seconds)

# COMMAND ----------

data_to_ingest = dbutils.fs.ls(bronze_path)
#print(data_to_ingest)
for data_file in data_to_ingest:
  start_time = datetime.now()
  print("***************************",data_file,"***************************************************")
  file_path = data_file.path
  file_name = data_file.name
  file_size_kb = data_file.size/1024
  file_ingestion_config = etl_params["ingestion"]["bronze_silver_mapping"]
  
  table_name = file_name.split(".")[0]
  read_options = {}
  write_command = "insert overwrite "
  table_format = "delta"
  src_file_format = "csv"

  if landing_default_read_params != None and landing_default_read_params != "":
    #print("Reading from Default Config Values")
    #print(landing_default_read_params)
    read_options = json.loads(landing_default_read_params.replace("'",'"'))
  if landing_default_file_format != None and landing_default_file_format != "":
    #print("Reading from Default Config Values")
    src_file_format = landing_default_file_format
  if bronze_default_file_format != None and bronze_default_file_format != "":
    #print("Reading from Default Config Values")
    src_file_format = bronze_default_file_format
  if silver_default_table_format != None and silver_default_table_format != "":
    table_format = silver_default_table_format  
  if default_silver_table_ops != None and default_silver_table_ops != "":
    write_command = default_silver_table_ops 

  #print("----------Default values--------------------------")
  #print(read_options,src_file_format,table_format,write_command)
  #print("Config Keys")
  #print(file_ingestion_config.keys())
  #print("test")
  #print("file_format" in list(file_ingestion_config[file_name].keys()))
  if "file_format" in list(file_ingestion_config[file_name].keys()):
    #print("Table Params")
    src_file_format = file_ingestion_config[file_name]["file_format"]

  if "table_name" in file_ingestion_config[file_name].keys():
    table_name = file_ingestion_config[file_name]["table_name"]
  if "read_options" in file_ingestion_config[file_name].keys():
    read_options = json.loads(file_ingestion_config[file_name]["read_options"].replace("'",'"'))
  if "write_command" in file_ingestion_config[file_name].keys():
    write_command = file_ingestion_config[file_name]["write_command"]
  if "table_format" in file_ingestion_config[file_name].keys():
    table_format = file_ingestion_config[file_name]["table_format"]
  #print("file_ingestion_config = ",file_ingestion_config)
  #print("Final Values")
  #print(read_options,src_file_format,table_format,write_command)
  print(table_name,silver_db,table_format,write_command)
  
  df = obj_dl.read_data(spark=spark,str_format=src_file_format,source_type=src_file_format,table_name="",query="",table_schema=silver_db,path=file_path,src_options=read_options)
  df_cols = df.columns
  for colum in df_cols:
    new_col_name = colum.replace(" ","").replace("-","_").replace("(","").replace(")","")#.replace("","")
    df = df.withColumnRenamed(colum,new_col_name)
  #display(df)
  silver_path  = "dbfs:/user/hive/warehouse/silver.db/" + table_name
  print("Silver Path = ",silver_path)
  
  obj_dl.write_data(df_data=df,spark=spark,table_name=table_name,data_path="",table_schema=silver_db,str_format=table_format,
                            write_command=write_command,str_merge_cols="",str_part_cols="",is_table=True)
  end_time = datetime.now()
  time_delta = end_time
  print("Data Saved in Silver; File Size(KB) = ",file_size_kb," KB; Time taken = ",((end_time-start_time).seconds)," Seconds;StartTime = ", start_time,"; EndTime = ",end_time)

# COMMAND ----------

#exit()

# COMMAND ----------

#src_options = {"header":"true","delimiter":"|","inferSchema":"true"}
##path = "dbfs:/FileStore/shared_uploads/bronze/sample.dat"
#silver_table = "sample"

# COMMAND ----------

##df = obj_dl.read_data(spark=spark,str_format="csv",source_type="csv",table_name="",query="",table_schema="default",path=path,src_options=src_options)

# COMMAND ----------

#obj_dl.write_data(df_data=df,spark=spark,table_name=silver_table,data_path="",table_schema="silver",str_format="delta",
#                            write_command="insert overwrite ",str_merge_cols="",str_part_cols="",is_table=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC use silver;
# MAGIC show tables;

# COMMAND ----------

spark.sql(""" select count(*) from silver.water_consumption 
          union 
          select count(*) from silver.electricity_consumption 
          union  
          select count(*) from silver.house_holds_slums""").show()


# COMMAND ----------

# MAGIC %sql
# MAGIC use silver;
# MAGIC desc detail silver.electricity_consumption;

# COMMAND ----------

dbutils.fs.ls("dbfs:/user/hive/warehouse/silver.db")

# COMMAND ----------

df_delta = spark.read.format("delta").load('dbfs:/user/hive/warehouse/silver.db/electricity_consumption/')
display(df_delta)

# COMMAND ----------

df_sql = spark.sql("select * from silver.electricity_consumption")
display(df_sql)

# COMMAND ----------

dbutils.fs.ls("dbfs:/user/hive/warehouse/silver.db/electricity_consumption/_delta_log/")

# COMMAND ----------

dbutils.fs.head('dbfs:/user/hive/warehouse/silver.db/electricity_consumption/_delta_log/00000000000000000000.json')