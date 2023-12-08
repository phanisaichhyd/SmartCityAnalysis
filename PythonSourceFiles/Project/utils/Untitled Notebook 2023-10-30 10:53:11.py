# Databricks notebook source
# MAGIC %run ./deltalake_utils

# COMMAND ----------

# MAGIC %run ./Iutils

# COMMAND ----------

class src_connector(Iops_factory):      
      @classmethod
      def get_src_obj(cls,config,source_type):
            src_cls = globals()[config['src_connector_class_mappings'][source_type]['class_name']]
            obj_src_cls = src_cls()
            return obj_src_cls