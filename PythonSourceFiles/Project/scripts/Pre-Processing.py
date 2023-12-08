# Databricks notebook source
# MAGIC %sql
# MAGIC use silver;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.electricity_consumption limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct City from silver.house_holds_slums where city in (select CityName from silver.electricity_consumption);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.water_consumption limit 5;

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

df = spark.sql("select * from silver.electricity_consumption")
cols_df = df.columns
for c in cols_df:
  df = df.withColumn(c,f.regexp_replace(f.col(c),"na",f.lit(None)))
display(df)

# COMMAND ----------

def replace_na_with_nulls(df):
  cols_df = df.columns
  for c in cols_df:
    df = df.withColumn(c,f.regex_replace(f.col(c),"na",f.lit(None)))
