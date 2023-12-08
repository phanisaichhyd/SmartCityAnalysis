# Databricks notebook source
# MAGIC %run ./deltalake_utils

# COMMAND ----------

# MAGIC %run ./api_connector

# COMMAND ----------

# MAGIC %run ./Iutils

# COMMAND ----------


class src_connector(Iops_factory):      
      @classmethod
      def get_src_obj(cls,config,source_type):
            #print("config = ",config)
            #print("source_type = ",source_type)
            src_cls = globals()[config['src_connector_class_mappings'][source_type]['class_name']]
            #print("src_cls = ",src_cls)
            obj_src_cls = src_cls()
            return obj_src_cls

# COMMAND ----------



# COMMAND ----------

print("utils_src_connector Imported")