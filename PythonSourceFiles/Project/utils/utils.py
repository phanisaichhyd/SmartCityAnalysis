# Databricks notebook source
import json
import logging

# COMMAND ----------

# MAGIC %run ./utils_src_connector

# COMMAND ----------

class config :       
    @classmethod      
    def get_config(cls,json_var):
      config = {}
      if json_var.strip() != "":
          config = json.loads(json_var)
      return config

# COMMAND ----------

class data_ops:
    def __init__(self,source_type,config) -> None:
        self.config = config
        self.src_obj = src_connector.get_src_obj(config,source_type)
    
    def get_source_obj(self):
        return self.src_obj 


# COMMAND ----------

class pylogger:
  def __init__(self, log_flag=True, log_file="", log_path=""):
        self.log_flag = log_flag
        self.log_path = log_path        
        self.log_file = log_file
        self.filename =  self.log_path+self.log_file
        self.format = '[%(asctime)s]: %(levelname)s: %(message)s'
        formatter = logging.Formatter(self.format)
        handler = logging.FileHandler(self.filename, 'w', 'utf-8')
        handler.setFormatter(formatter)
        logger = logging.getLogger(self.log_file)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        logger.addHandler(handler)
        self.logger_ = logger
        print("filename = ",self.filename)

  def print_log(self,message = "",error=False):
    if error:
      if self.log_flag:
        self.logger_.error(message)
        print('**** ERROR: ' + message + ' ***')
    else:
      if self.log_flag:
        self.logger_.info(message)
        print('>> ' + message + ' <<')

  def move_log_file(self, temp_log_file_path,log_path):
    dbutils.fs.mv(temp_log_file_path,log_path)

# COMMAND ----------

print("Notebook ran")