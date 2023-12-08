# Databricks notebook source
# MAGIC %md
# MAGIC ## Electricity Clustering

# COMMAND ----------

from pyspark.sql.functions import col, when, count, sum, avg

# COMMAND ----------

# MAGIC %run ../utils/model_utils

# COMMAND ----------

df = spark.sql("select * from silver.electricity_consumption")

# COMMAND ----------

display(df)

# COMMAND ----------

df, numerical_cols = data_preprocessing(df, null_threshold_percentage = 0.5)

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.filter((col('CityName') != 'Port Blair') & (col('CityName') != 'DHARAMSHALA'))
display(df)

# COMMAND ----------

feature_cols = numerical_cols
cluster_df = KmeanCluster(df, feature_cols)

# COMMAND ----------

silhouette_score, pca_df = score_and_pca(cluster_df)

# COMMAND ----------

displayHTML(f"<h1>Silhouette Score: {silhouette_score:.2f}</h1>")

# COMMAND ----------

plot_clusters_pca_result(pca_df)

# COMMAND ----------

display(cluster_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cluster-wise City Count

# COMMAND ----------

grouped_data = cluster_df.groupBy("Clusters").agg(
    count("*").alias("City Count")
)

display(grouped_data)

# COMMAND ----------

grouped_data = cluster_df.groupBy("Clusters").agg(
    avg("ConsumptionofElectricityinlakhunitsTotalConsumption").alias("AverageElectricityConsumption")
)

pandas_df = grouped_data.select('Clusters', 'AverageElectricityConsumption').toPandas()

# Plot a city-wise stacked bar plot
ax = pandas_df.set_index('Clusters').plot(kind='bar', stacked=True, figsize=(10, 6))
plt.xlabel('Clusters')
plt.ylabel('Average Consumption')
ax.get_yaxis().get_major_formatter().set_scientific(False)
plt.show()

# COMMAND ----------

df_report = cluster_df.withColumn("Electricity_consumption_cluster_Type",
                                  when (col("Clusters") == 0,"Medium")\
                                .when(col("Clusters") == 1,"Small")\
                                    .otherwise("Large")
                                  )

display(df_report)

# COMMAND ----------

grouped_data = df_report.groupBy("Electricity_consumption_cluster_Type").agg(
    avg("ConsumptionofElectricityinlakhunitsTotalConsumption").alias("AverageElectricityConsumption")
)

pandas_df = grouped_data.select('Electricity_consumption_cluster_Type', 'AverageElectricityConsumption').toPandas()
pandas_df = pandas_df.sort_values('AverageElectricityConsumption', ascending = False)

# Plot a city-wise stacked bar plot
ax = pandas_df.set_index('Electricity_consumption_cluster_Type').plot(kind='bar', stacked=True, figsize=(10, 6))
plt.xlabel('Clusters')
plt.ylabel('Average Electricity Consumption')
ax.get_yaxis().get_major_formatter().set_scientific(False)
plt.show()

# COMMAND ----------

# MAGIC %run ../config/etl_config

# COMMAND ----------

# MAGIC %run ../utils/utils

# COMMAND ----------


source_type= "deltalake"
code_config = config.get_config(json_var = connector_config)
etl_params = config.get_config(json_var = processing_config)
obj_data_ops = data_ops(source_type,code_config)
obj_dl = obj_data_ops.get_source_obj()
obj_dl

# COMMAND ----------

obj_dl.write_data(df_data=df_report,spark=spark,table_name="electricity_clusters",data_path="",table_schema="gold",str_format="delta",
                            write_command="insert overwrite",str_merge_cols="",str_part_cols="",is_table=True)

# COMMAND ----------

df = spark.sql("select * from gold.electricity_clusters")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

