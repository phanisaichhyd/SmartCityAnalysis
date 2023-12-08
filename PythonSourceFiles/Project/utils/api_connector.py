# Databricks notebook source
import json
import requests
import os
import sys
import subprocess
from os.path import join as joinpath, dirname, basename
import pandas as pd
from unicodedata import normalize
import re
import pyspark

# COMMAND ----------

#%run ../utils/utils

# COMMAND ----------

class GetData(Idata_ops):
    valid_fields = ['ward name', 'city', 'api'] #all col names should be in lower
    def __init__(self, medium = None, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        return None
    
    def from_file(self, file_path, *args, **kwargs):
        format = basename(file_path).split('.')[1]
        return spark.read.format(format).option("header", "true").load(file_path)
    
    def check_input(self, attr, do_raise = True):
        if not hasattr(self, attr):
            if do_raise:
                raise ValueError(f'Attribute name is wrong or value has not provided for: {attr}')
            return None
        return getattr(self, attr, None)
    
    def normalize_fields(self, fields, replace = None):
        out = []
        for field in fields:
            norm = ' '.join(field.split()) #remove multiple spaces
            norm = normalize('NFKD', field).encode('ASCII', 'ignore').decode('utf-8') #if header is in devnagri etc
            norm = re.sub(r'[^a-zA-Z0-9\s]', '', field) #special chars and numbers
            norm = norm.replace('/','').replace(':','') 
            out.append(norm.lower())
        return out
        
    def format_df(self, df, cols_map):
        out = {}
        if not GetData.valid_fields:
            raise ValueError(f'Please provide and field names Getdata.valid_fields = [<valid_column_names>]')
        for k, v in cols_map.items():
            if k in GetData.valid_fields:
                out[k] = dict(df[v])
        
        return pd.DataFrame(out) if out else {}
    
    @classmethod
    def mergs_dfs(cls, directory_path):
        spark_df = spark.read.option("delimiter", ";").csv(directory_path, header=True, inferSchema=True)

        return spark_df
        
    def read_data(self, api, key = None): #spark DF
        try: 
            out = json.loads(subprocess.run(api, check=True, capture_output=True, text=True, shell = True).stdout)
        except subprocess.CalledProcessError as e:
            print(f"Error executing cURL command. Return code: {e.returncode}")
            print("Error output:", e.output)
            return None
        if key:
            out = out.get(key, {})
        out = pd.json_normalize(out)
        print(f'Available columns: {list([i.lower() for i in out.columns])}')
        out = self.format_df(out, dict(zip(self.normalize_fields(out.columns), out.columns)))
        print(f'Samples: {out.head()}')
        return spark.createDataFrame(out)

    def write_data(self, df, path):
        if isinstance(df, pyspark.sql.dataframe.DataFrame):
            df = df.toPandas()
        row_count = len(df.index)
        if row_count <=0:
            print('Got nothing to write in..')
            return
        df.to_csv(path)
        print(f'Data frame saved as csv at {path}')
        
    
    