# Databricks notebook source
# MAGIC %md
# MAGIC ## Households in Slums Clustering

# COMMAND ----------

from pyspark.sql.functions import col, when, count, sum, avg

# COMMAND ----------

# MAGIC %run ../utils/model_utils

# COMMAND ----------

df = spark.sql("select * from silver.house_holds_slums")

# COMMAND ----------

display(df)

# COMMAND ----------

df, numerical_cols = data_preprocessing(df, null_threshold_percentage = 0.5)

# COMMAND ----------

df = df.drop('Zone', 'WardName', 'WardNo.')
df = df.groupBy("City").agg(sum('NoNotifiedSlums').alias('NoNotifiedSlums'),
                            sum('NoOfRecognisedSlums').alias('NoOfRecognisedSlums'),
                            sum('NoOfIdentfiedSlums').alias('NoOfIdentfiedSlums'),
                            sum('SlumPopulationTotal').alias('SlumPopulationTotal'),
                            sum('SlumPopulationMale').alias('SlumPopulationMale'),
                            sum('SlumPopulationFemale').alias('SlumPopulationFemale'),
                            sum('PopulationSCCategory').alias('PopulationSCCategory'),
                            sum('PopulationSTCategory').alias('PopulationSTCategory'),
                            avg('LiteracyRatePercentage').alias('LiteracyRatePercentage')
                            )

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.withColumn("SlumPopulationMale", when((col("SlumPopulationTotal") > 0) & (col("SlumPopulationMale") == 0), 0.52 * col("SlumPopulationTotal")).otherwise(col("SlumPopulationMale"))) \
    .withColumn("SlumPopulationFemale", when((col("SlumPopulationTotal") > 0) & (col("SlumPopulationFemale") == 0), 0.48 * col("SlumPopulationTotal")).otherwise(col("SlumPopulationFemale")))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### City-wise Slum Population

# COMMAND ----------

top_10_cities_population = df.orderBy("SlumPopulationTotal", ascending=False).limit(10)
pandas_df = top_10_cities_population.select('City', 'SlumPopulationMale','SlumPopulationFemale').toPandas()

# Plot a city-wise stacked bar plot
ax = pandas_df.set_index('City').plot(kind='bar', stacked=True, figsize=(10, 6))
plt.xlabel('City')
plt.ylabel('Population')
ax.get_yaxis().get_major_formatter().set_scientific(False)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC top_10_cities_literacy = df.orderBy("LiteracyRatePercentage", ascending=False).limit(10)
# MAGIC pandas_df = top_10_cities_population.select('City', 'LiteracyRatePercentage').toPandas()
# MAGIC
# MAGIC # Plot a city-wise stacked bar plot
# MAGIC ax = pandas_df.set_index('City').plot(kind='bar', figsize=(10, 6))
# MAGIC plt.xlabel('City')
# MAGIC plt.ylabel('Literacy rate')
# MAGIC ax.get_yaxis().get_major_formatter().set_scientific(False)
# MAGIC plt.show()

# COMMAND ----------

#top_10_cities_literacy = df.orderBy("LiteracyRatePercentage", ascending=False).limit(10) pandas_df = top_10_cities_population.select('City', 'LiteracyRatePercentage').toPandas()

# COMMAND ----------

#display(top_10_cities_literacy)

# COMMAND ----------

feature_cols = ['SlumPopulationTotal', 'SlumPopulationMale', 'PopulationSCCategory', 'PopulationSTCategory', 'LiteracyRatePercentage']
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

df.withColumn("SlumPopulationMale", when((col("SlumPopulationTotal") > 0) & (col("SlumPopulationMale") == 0), 0.52 * col("SlumPopulationTotal")).otherwise(col("SlumPopulationMale")))

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
    avg("SlumPopulationMale").alias("SlumPopulationMale"),
    avg("SlumPopulationFemale").alias("SlumPopulationFemale")
)

pandas_df = grouped_data.select('Clusters', 'SlumPopulationMale','SlumPopulationFemale').toPandas()
pandas_df = pandas_df.sort_values('SlumPopulationMale', ascending = False)
# Plot a city-wise stacked bar plot
ax = pandas_df.set_index('Clusters').plot(kind='bar', stacked=True, figsize=(10, 6))
plt.xlabel('Clusters')
plt.ylabel('Population')
ax.get_yaxis().get_major_formatter().set_scientific(False)
plt.show()

# COMMAND ----------

df_report = cluster_df.withColumn("Slum_Cluster_Type",
                                  when (col("Clusters") == 0,"Medium")\
                                .when(col("Clusters") == 1,"Small")\
                                    .otherwise("Large")
                                  )

display(df_report)

# COMMAND ----------

grouped_data = df_report.groupBy("Slum_Cluster_Type").agg(
    avg("SlumPopulationMale").alias("SlumPopulationMale"),
    avg("SlumPopulationFemale").alias("SlumPopulationFemale")
)

pandas_df = grouped_data.select('Slum_Cluster_Type', 'SlumPopulationMale','SlumPopulationFemale').toPandas()
pandas_df = pandas_df.sort_values('SlumPopulationMale', ascending = False)
# Plot a city-wise stacked bar plot
ax = pandas_df.set_index('Slum_Cluster_Type').plot(kind='bar', stacked=True, figsize=(10, 6))
plt.xlabel('Clusters')
plt.ylabel('Population')
ax.get_yaxis().get_major_formatter().set_scientific(False)
plt.show()

# COMMAND ----------



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



# COMMAND ----------

obj_dl.write_data(df_data=df_report,spark=spark,table_name="household_slums_clusters",data_path="",table_schema="gold",str_format="delta",
                            write_command="insert overwrite",str_merge_cols="",str_part_cols="",is_table=True)

# COMMAND ----------

df = spark.sql("select * from gold.household_slums_clusters")
display(df)

# COMMAND ----------

df.printSchema()