# Databricks notebook source
dbutils.fs.mkdirs("dbfs:/FileStore/shared_uploads/landing_zone")
dbutils.fs.mkdirs("dbfs:/FileStore/shared_uploads/bronze")
spark.sql("create  database if not exists silver")
spark.sql("create  database if not exists gold")

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs "dbfs:/FileStore/shared_uploads/landing_zone";
# MAGIC mkdirs "dbfs:/FileStore/shared_uploads/bronze"

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs "dbfs:/FileStore/shared_uploads/bronze"

# COMMAND ----------

# MAGIC %sql
# MAGIC create  database if not exists silver;
# MAGIC create  database if not exists gold;

# COMMAND ----------

def check_and_remove_path(path):
  print("Removing Path",path)
  try:
    lst_files = dbutils.fs.ls(path)
    print("Checking length")
    if len(lst_files)>0:
      print("RM operation")
      dbutils.fs.rm(path,True)
  except Exception as ex :
    print(ex)
    pass

silver_db_path = "dbfs:/user/hive/warehouse/silver.db/"
silver_tables = dbutils.fs.ls(silver_db_path)
if len(silver_tables) >0:
  for silver_table in silver_tables:
    print(silver_table.path)
    check_and_remove_path(silver_table.path)
#silver_files = dbutils.fs.ls()


# COMMAND ----------

dbutils.fs.ls(silver_db_path)

# COMMAND ----------

def check_and_remove_path(path):
  print("Removing Path",path)
  try:
    lst_files = dbutils.fs.ls(path)
    print("Checking length")
    if len(lst_files)>0:
      print("RM operation")
      dbutils.fs.rm(path,True)
  except Exception as ex :
    print(ex)
    pass

gold_db_path = "dbfs:/user/hive/warehouse/gold.db/"
gold_tables = dbutils.fs.ls(gold_db_path)
if len(gold_tables) >0:
  for gold_table in gold_tables:
    print(gold_table.path)
    check_and_remove_path(gold_table.path)
#silver_files = dbutils.fs.ls()


# COMMAND ----------

dbutils.fs.help("ls")

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop database  silver cascade;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc database silver;