# Databricks notebook source
import datetime

# COMMAND ----------

#dbutils.widgets.text("bronze_format","csv")
#dbutils.widgets.text("silver_db","silver")
#dbutils.widgets.text("gold_db","gold")
#dbutils.widgets.text("config_file","etl_config")
dbutils.widgets.text("config","etl_config")
dbutils.widgets.text("logger","logger")

# COMMAND ----------

# MAGIC %run ../utils/utils

# COMMAND ----------

dbutils.widgets.remove("config_file")

# COMMAND ----------

processing_config = dbutils.widgets.get("config")
logger = dbutils.widgets.get("logger")

# COMMAND ----------

# MAGIC %run ../config/etl_config $x=1000

# COMMAND ----------

processing_config = config.get_config(processing)
log_file_path = processing_config['temp_log_file_path']
current_ts = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
log_file_name = "log_file_" + str(current_ts) + '.log'
logger = pylogger(log_flag=True, log_file=log_file_name, log_path=log_file_path)

# COMMAND ----------



# COMMAND ----------

processing_config

# COMMAND ----------

landing_files = dbutils.fs.ls(processing_config["ingestion"]["landing"]["path"])
bronze_files = dbutils.fs.ls(processing_config["ingestion"]["bronze"]["path"])
landing_files

# COMMAND ----------

bronze_files = [files.name for files in bronze_files]
logger.print_log("Cleaning up Bronze Layer")
for f in bronze_files:
  dbutils.fs.rm(bronze_path + f)
landing_files = [files.name for files in landing_files]
logger.print_log("Moving Landing files to bronze layer")
for f in landing_files:
  logger.print_log("Moving " + f + " from landing to bronze")
  dbutils.fs.cp(landing_path + f ,bronze_path)
logger.print_log("All files are moved from landing area to bronze")
dbutils.fs.ls(bronze_path)

# COMMAND ----------

curr_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(curr_path)
log_path = "/".join(curr_path.split("/")[:-2]) + "/logs"

# COMMAND ----------

bronze_files = [files.name for files in bronze_files]


# COMMAND ----------

for bronze_file in bronze_files:
  silver_table = processing_config["ingestion"]["bronze_silver_mapping"][bronze_file]
  print(silver_table)

# COMMAND ----------

