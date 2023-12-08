# Databricks notebook source
from pyspark.sql.functions import col, when, count
from pyspark.sql.types import FloatType
from pyspark.ml.feature import VectorAssembler, MinMaxScaler, PCA
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import ClusteringEvaluator
import matplotlib.pyplot as plt
import re

# COMMAND ----------

def infer_data_type(value):
    """Determines whether a value can be converted to float or not
    
    Args:
        value: a string
    Returns:
        datatype object float or string
    """
    try:
        # Try converting to double
        return type(float(value))
    except ValueError:
        # Return as string if conversion fails
        return str

# COMMAND ----------

def infer_schema(data_frame):
    """Determines which columns can be converted to float datatype
    
    Args:
        data_frame: a spark dataframe
    Returns:
        An updated schema for the dataframe with float columns if any
    """

    new_schema = []
    for col_name in data_frame.columns:
        # Collect unique values in the column
        unique_values = data_frame.select(col(col_name)).distinct().collect()

        # Infer data type based on unique values
        inferred_type = None
        for row in unique_values:
            value = row[0]
            inferred_type = infer_data_type(value)

            # Break if a suitable type is found
            if inferred_type is not None:
                break

        # Assign the inferred type or default to StringType
        new_type = (
            FloatType() if inferred_type == float else
            "string"
        )

        # Add the column name and its inferred type to the new schema
        new_schema.append((col_name, new_type))

    return new_schema

# COMMAND ----------

def nice_case(s: str, sep: str='') -> str:
    """Converts most strings to NiceCase or Nice{sep}Case.
    
    Args:
        s: Input string to be nice-ified
        sep: Separator to be used for nice-ification
    
    Returns:
        Nice-ified string
    """
    
    s = re.sub ("[^A-Za-z0-9]+", ' ', s)
    s = re.sub(r"( )+", " ", s)
    s = s.strip()
    components = [''.join([word[0].upper(), word[1:]]) for word in s.split(sep=' ')]
    
    return sep.join(components)

# COMMAND ----------

def data_preprocessing(df, null_threshold_percentage = 0.3):
    """Preprocesses the dataframe and determines all numerical columns

    Args:
        df: Spark dataframe to be processed
    Returns:
        The processed dataframe and list of all numerical columns
    """
    
    #Nicefy all column names
    for colname in df.columns:
        df = df.withColumnRenamed(colname, nice_case(colname))

    #Replace 'NA' string with None. This will help in correctly identifying the schema
    for col_name in df.columns:
        df = df.withColumn(col_name, when(col(col_name) == 'NA', None).otherwise(col(col_name)))

    #Dropping columns with null percentage more than the threshold
    num_rows = df.count()
    null_counts = df.select([(count(when(col(c).isNull(), c))/num_rows).alias(c) for c in df.columns]).collect()[0].asDict() 
    col_to_drop = [k for k, v in null_counts.items() if v > null_threshold_percentage]
    df = df.drop(*col_to_drop)

    # Infer the schema
    new_schema = infer_schema(df)

    #To store column names of all numerical columns
    numerical_cols = []

    # Apply the new schema to the DataFrame and create a list of numerical columns
    for col_name, col_type in new_schema:
        df = df.withColumn(col_name, col(col_name).cast(col_type))
        if col_type == FloatType():
            numerical_cols.append(col_name)

    #Fill all null values with 0, since spark ML models cannot us null values
    df = df.fillna(0, numerical_cols)

    return df, numerical_cols

# COMMAND ----------

def KmeanCluster(df, feature_cols, n_clusters = 3, seed = 1):
    """Clusters the data points using K-means clustering based on the feature columns

        Args:
            df: Spark dataframe which contains the features
            feature_cols: A list of feature column names
            n_clusters: Number of cluster to be created
            seed: Seed value to initialise the cluster object
        Returns:
            A spark dataframe with vectorized features and cluster predictions
    """    
    #Assemble all feature columns into a vector
    vecAssembler = VectorAssembler(inputCols = feature_cols, outputCol="raw_features").setHandleInvalid("skip")

    #Scale the feature vector
    scaler = MinMaxScaler(inputCol="raw_features", outputCol="features")

    #Initialize the cluster object
    kmeans = KMeans(featuresCol = "features", predictionCol="Clusters").setK(n_clusters).setSeed(seed)

    #Initialize the pipeline object
    pipeline= Pipeline(stages = [vecAssembler, scaler, kmeans])

    #Fit the pipeline to the data
    pipelineModel = pipeline.fit(df)
    
    #Cluster the data
    cluster_df = pipelineModel.transform(df)

    return cluster_df

# COMMAND ----------

def score_and_pca(df):
    evaluator = ClusteringEvaluator()
    evaluator.setPredictionCol("Clusters")
    silhouette_score = evaluator.evaluate(cluster_df.select(['features', 'Clusters']))

    pca = PCA(k=2, inputCol="features", outputCol="pca_features")

    # Fit the PCA model to the data and transform
    pca_model = pca.fit(cluster_df)
    pca_result = pca_model.transform(cluster_df)

    return silhouette_score, pca_result

# COMMAND ----------

def plot_clusters_pca_result(pca_result):
    pandas_df = pca_result.select("pca_features", "Clusters").toPandas()

    scatter = plt.scatter(
                    pandas_df['pca_features'].apply(lambda x: x[0]),
                    pandas_df['pca_features'].apply(lambda x: x[1]),
                    c=pandas_df['Clusters'], 
                    cmap='viridis_r', marker='o', edgecolors='black'
                )
    
    plt.title('K-means Clustering with PCA')
    plt.xlabel('Principal Component 1')
    plt.ylabel('Principal Component 2')
    legend_cluster = plt.legend(*scatter.legend_elements(), loc="upper right", title="Clusters")
    plt.show()