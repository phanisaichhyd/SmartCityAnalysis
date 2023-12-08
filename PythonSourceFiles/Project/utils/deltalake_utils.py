# Databricks notebook source
# MAGIC %run ./Iutils

# COMMAND ----------

from pyspark.sql import functions as f
from delta.tables import *
from pyspark.sql import DataFrame

class deltalake(Idata_ops): 
    '''
    Class to perform Data Operations on data lake
    '''
 

    @classmethod
    def __table_exists(cls,spark,table_name,table_schema) -> bool:
        """
        returns if the table is present in the schema
        """
        spark_table_list = spark.catalog.listTables(table_schema)

        return (table_name.lower() in [table.name for table in spark_table_list])
    
    @classmethod
    def __gen_query_cond(cls,src_alias,dest_alias,list_cols) -> str:
        """
        The functions iterate through the list of columns and generate query condition to be used in 
        merge. Aliases for source and destination are used as passed in paramters
        """

        str_query_cond = ""
        for cols in list_cols:
            str_query_cond = str_query_cond + ' and ' + dest_alias + "." + cols + " = " + src_alias + "." + cols 

        str_query_cond = str_query_cond[4:]
        print(str_query_cond)

        return str_query_cond
    
    @classmethod
    def __gen_part_condition(cls,df_data,list_part_cols) -> str:

        str_part_cond = ""
        str_part_value= ""

        for part_col in list_part_cols:
            str_part_value = str("'" + df_data.agg(f.concat_ws("','", f.collect_set(df_data[part_col]))).first()[0])
            print("Partition Value for part_col")
            print(str_part_value)
            if str_part_value != "":
                str_part_cond =  str_part_cond + ' and existing.' + part_col + ' in (' + str_part_value + "'" + ')'

        str_part_cond = str_part_cond[4:]

        print("Partition Condition is ")
        print(str_part_cond)
        return str_part_cond

    
    @classmethod
    def __create_insert_table(cls,df_data,table_name,table_schema="default",table_format="delta",str_part_cols=""):
        """
        Create Table and insert Data into it using dataframe
        """

        if str_part_cols != None and str(str_part_cols) != "":
            df_data.write.partitionBy(str_part_cols.split(",")).format(table_format).saveAsTable(table_schema + "." + table_name)   
                
        else:
             df_data.write.format(table_format).saveAsTable(table_schema + "." + table_name)
               

    @classmethod
    def __write_path(cls,df_data,data_path,write_command="append",str_format="delta",str_part_cols="") ->None:
        """
        Inserts Data into the specified location. Supports both append and Overwrite
        """ 
        row_count = df_data.count()
        if row_count <=0:
            return
        
        if str_part_cols != None and str(str_part_cols) != "":
            df_data.write.partitionBy(str_part_cols.split(",")).mode(write_command).format(str_format).save(data_path)
        else:
            df_data.write.mode(write_command).format(str_format).save(data_path)


    @classmethod
    def __write_table(cls,spark,insert_command,table_name,df_data,table_schema="default",table_format="delta",str_part_cols="") ->None:
        """
        Inserts Data into the table. Supports both Insert Into and Insert Overwrite
        """
        row_count = df_data.count()
        if row_count <=0:
            return
        
        insert_query = ""
        table_exists = cls.__table_exists(spark = spark,table_name=table_name,table_schema=table_schema)
        
        if str_part_cols != None and str(str_part_cols) != "":
            df_data = df_data.select([col for col in df_data.columns if col not in str_part_cols.split(",")] + str_part_cols.split(","))
        
        df_data.createOrReplaceTempView("df")

        if table_exists:

            print("Table Exists. Hence performing " + insert_command)

            insert_query = insert_command + " table " + table_schema + "." + table_name

            if str_part_cols != None and str(str_part_cols) != "":                
                insert_query = insert_query + " partition(" + str_part_cols + ") "
            
            insert_query = insert_query + " select * from df "
            print(insert_query)
            spark.sql(insert_query)

        else:
               print("Table doesn't exists. Hence Creating the table and inserting Data")
               cls.__create_insert_table(df_data,table_name,table_schema,table_format,str_part_cols)


    @classmethod
    def __merge_data(cls,spark,str_merge_cols,delta_table,df_data,table_schema="default",table_format="delta",str_part_cols="",is_table=True) ->None:
        """
            Merge Data into the existing Table Data table_name
        """
        merge_condition = ""
        str_part_cond = ""
        table_exists = True

        row_count = df_data.count()
        if row_count <=0:
            return

        if str_merge_cols == None or str_merge_cols == "":
            return

        if is_table :
            table_exists = cls.__table_exists(spark = spark,table_name=delta_table,table_schema=table_schema)
        else:
            table_exists = DeltaTable.isDeltaTable(spark, delta_table)
        
        if table_exists:
            print("Table Exists. Hence Merging the Data(upsert)")

            merge_condition = cls.__gen_query_cond(src_alias="updates",dest_alias="existing",list_cols=str_merge_cols.split(","))

            if str_part_cols != None and str(str_part_cols) != "":
                str_part_cond = cls.__gen_part_condition(df_data,str_part_cols.split(","))
            
            merge_condition = merge_condition + " and " + str_part_cond

            
            if is_table:
                table = table_schema + "." + delta_table
                dt = DeltaTable.forName(spark,table)
            else:
                dt = DeltaTable.forPath(spark,delta_table)

            dt.alias("existing").merge(df_data.alias("updates"),merge_condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            
        else:
            print("Table doesn't exists. Hence Creating the table and inserting Data")
            if is_table:
                cls.__create_insert_table(df_data=df_data,table_name=delta_table,table_schema=table_schema,
                                               table_format=table_format,str_part_cols=str_part_cols)
            else:
                cls.__write_path(df_data=df_data,data_path=delta_table,write_command="overwrite",
                              str_format=table_format,str_part_cols=str_part_cols)
        

    
    def write_data(self,df_data,spark=spark,table_name="",data_path="",table_schema="default",str_format="delta",
                            write_command="insert into ",str_merge_cols="",str_part_cols="",is_table=True) -> None:
        """
        Writes Data of Dataframe to either Table or a path
        """
        if is_table:
            if write_command.lower().__contains__("insert"):
                self.__write_table(spark=spark,insert_command=write_command,table_name=table_name,df_data=df_data,
                                   table_schema=table_schema,table_format=str_format,str_part_cols=str_part_cols)
            else:
                print("Merge Command")
                self.__merge_data(spark=spark,str_merge_cols=str_merge_cols,delta_table=table_name,df_data=df_data,
                                  table_schema=table_schema,table_format=str_format,str_part_cols=str_part_cols,is_table=is_table) 
        else:
            if write_command.lower().__contains__("insert"):
                self.__write_path(df_data=df_data,data_path=data_path,write_command=write_command,
                              str_format=str_format,str_part_cols=str_part_cols)
            else:
                print("Merge Command")
                self.__merge_data(spark=spark,str_merge_cols=str_merge_cols,delta_table=data_path,df_data=df_data,
                                  table_schema=table_schema,table_format=str_format,str_part_cols=str_part_cols,is_table=is_table) 
    
    def read_data(self,spark=spark,str_format="delta",source_type="table",table_name="",query="",table_schema="default",path="",src_options={}) -> DataFrame:
        
        if source_type.lower() == "table":
            print("Reading Data from SQL")
            if query.strip() != "":
                print("Reading data from Query")
                df_data = spark.sql(query)
            else:
                print('Reading data from Table')
                df_data = spark.sql("select * from " + table_schema + "." + table_name )        
        else:
            df_data = self.__read_data_others(spark,str_format,path=path,src_options=src_options)

        return df_data

    
    @classmethod
    def __read_data_others(cls,spark,str_format="delta",path="",src_options={}) -> DataFrame:
        df = spark.read.format(str_format).options(**src_options).load(path)
        return df
        

