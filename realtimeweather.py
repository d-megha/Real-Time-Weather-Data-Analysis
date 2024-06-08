# Databricks notebook source
storage_account_name = "realtimelanding"
client_id            = dbutils.secrets.get(scope="realtime-databricks-scope", key="realtime-databricks-app-client-id")
tenant_id            = dbutils.secrets.get(scope="realtime-databricks-scope", key="realtime-databricks-app-tenant-id")
client_secret        = dbutils.secrets.get(scope="realtime-databricks-scope", key="realtime-databricks-app-client-secret")

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

dbutils.fs.unmount("/mnt/realtimelanding/dbkslanding/")

mount_adls("dbkslanding")

# COMMAND ----------

#importing necessary libraries
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

#function to create delta table

def createDeltaTable(database_name, table_name, table_schema, table_location):
    #***This method creats a delta table with the passed-in name, schema and location***
    
    # we create a simple empty data frame
    df = spark.createDataFrame(spark.sparkContext.emptyRDD(), table_schema)
    df.printSchema()
    
    # create the underlying storage for our table
    df.write.format("delta").mode('overwrite').save(table_location)
    
    # Drop the table and the database if it already exists. 
    # then re create the database
    spark.sql(f"DROP TABLE IF EXISTS {database_name}.{table_name}")
    spark.sql(f"DROP DATABASE IF EXISTS {database_name}")
    spark.sql(f"CREATE DATABASE {database_name}")
    
    # re create the table
    spark.sql(f"CREATE TABLE {database_name}.{table_name} USING DELTA LOCATION '{table_location}'")
    
    # Verify that everything worked by doing a select on the table, we should get an empty dataframe
    # back with correct schema
    emptyDF = spark.sql(f"SELECT * FROM {database_name}.{table_name}")
    display(emptyDF)
    
    # list the details of the table
    describe_df = spark.sql(f"DESCRIBE {database_name}.{table_name}")
    display(describe_df)

# COMMAND ----------


database_name = 'realtime_streaming_db'
delta_table_name = 'weather_landing_dlt_tbl'
delta_location = f'/mnt/realtimelanding/dbkslanding/dbks_realtime_landing/{delta_table_name}.delta'

#define the delta table schema
schema = StructType()\
            .add("name", StringType(),False)\
            .add("country_cd", StringType(),True)\
            .add("id", IntegerType(),True)\
            .add("weather_dt", TimestampType(),True)\
            .add("timezone", StringType(),False)\
            .add("lat", DoubleType(),True)\
            .add("lon", DoubleType(),True)\
            .add("temp", DoubleType(),True)\
            .add("feels_like", DoubleType(),True)\
            .add("temp_min", DoubleType(),True)\
            .add("temp_max", DoubleType(),True)\
            .add("pressure", DoubleType(),True)\
            .add("humidity", DoubleType(),True)\
            .add("wind_speed", DoubleType(),True)\
            .add("wind_degree", DoubleType(),True)

# COMMAND ----------

#Create the delta table
createDeltaTable(database_name,delta_table_name,schema,delta_location)

# COMMAND ----------

# Read Event Hub's stream
conf = {}

conf["eventhubs.connectionString"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt("Endpoint=sb://<Eventhub name>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<shared access key>=;EntityPath=ehub_databricks_read_01")

input_stream_df = (
  spark
    .readStream
    .format("eventhubs")
    .options(**conf)
    .load()
)
