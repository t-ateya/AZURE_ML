# Databricks notebook source
# Important the relevant data types
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

orders_streaming_path = "/mnt/streaming/streaming_dataset/orders_streaming.csv"

orders_schema = StructType(
    [
        StructField("ORDER_ID", IntegerType(), False),
        StructField("ORDER_DATETIME", StringType(), False),
        StructField("CUSTOMER_ID", IntegerType(), False),
        StructField("ORDER_STATUS", StringType(), False),
        StructField("STORE_ID", IntegerType(), False)
    ]
)

orders_sdf= spark.read.csv(path=orders_streaming_path, header = True, schema=orders_schema)

# COMMAND ----------

orders_sdf.display()

# COMMAND ----------

orders_sdf = spark.readStream.csv(orders_streaming_path, orders_schema, header=True)



# COMMAND ----------

orders_sdf.display()

# COMMAND ----------

orders_sdf.isStreaming

# COMMAND ----------

