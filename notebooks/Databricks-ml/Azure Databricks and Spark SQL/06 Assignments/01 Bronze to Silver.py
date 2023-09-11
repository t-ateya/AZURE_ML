# Databricks notebook source
# MAGIC %md
# MAGIC # Assignment 1- Broze to Silver (Solution)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ORDERS
# MAGIC Save a table called ORDERS in the silver layer, it should contain the following columns:
# MAGIC - ORDER_ID, type INTEGER
# MAGIC - ORDER_TIMESTAMP, type TIMESTAMP
# MAGIC - CUSTOMER_ID, type INTEGER
# MAGIC - STORE_NAME, type STRING
# MAGIC
# MAGIC The results should only contain records where the order status is 'COMPLETE'
# MAGIC The file should be stored in a PARQUET format
# MAGIC
# MAGIC Hint1: You will need to initially read in the ORDER_DATETIME column and then convert it to timestamp using to_timestamp
# MAGIC
# MAGIC Hint2: You will need merge the orders and stores tables from the bronze layer

# COMMAND ----------

# Import the relevant csv file
from pyspark.sql.types import IntegerType, DoubleType, StringType, StructField, StructType

# COMMAND ----------

orders_path = "/FileStore/tables/bronze/orders.csv"

# COMMAND ----------

orders_schema = StructType([
    
    StructField("ORDER_ID", IntegerType(), False),
    StructField("ORDER_DATETIME", StringType(), False),
    StructField("CUSTOMER_ID", IntegerType(), False),
    StructField("ORDER_STATUS", StringType(), False),
    StructField("STORE_ID", IntegerType(), False)
    ]
)

orders = spark.read.csv(path=orders_path, header = True, schema=orders_schema)

# COMMAND ----------

orders.display()

# COMMAND ----------

# Importing the to_timestamp function
from pyspark.sql.functions import to_timestamp

# COMMAND ----------

# Converting the order_datetime column to timestamp and aliasing the name as 'order_timestamp'

orders = orders.select('ORDER_ID', \
            to_timestamp(orders["ORDER_DATETIME"], "dd-MMM-yy kk.mm.ss.SS").alias('ORDER_TIME'), \
            'CUSTOMER_ID', \
            'ORDER_STATUS', \
            'STORE_ID'
    )

# COMMAND ----------

# Confirming the data types
orders.dtypes

# COMMAND ----------

# filtering the records to display only 'COMPLETE' orders
orders = orders.filter(orders['order_status']=='COMPLETE')

# COMMAND ----------

# Reading the stores csv file

store_path = "/FileStore/tables/bronze/stores.csv"

store_schema = StructType([
    StructField("STORE_ID", IntegerType(), False),
    StructField("STORE_NAME", StringType(), False),
    StructField("WEB_ADDRESS", StringType(), False),
    StructField("LATITUDE", DoubleType(), False),
    StructField("LONGITUDE", DoubleType(), False)
    ]
)



# COMMAND ----------

stores = spark.read.csv(path=store_path, header=True, schema=store_schema)

# COMMAND ----------

stores.display()

# COMMAND ----------

# Joining the orders and stores via 'left' join, the orders table is the left table.
orders = orders.join(stores, orders['store_id']==stores['store_id'], 'left').select('ORDER_ID', 'ORDER_TIME', 'CUSTOMER_ID', 'STORE_NAME')

# COMMAND ----------

orders.display()

# COMMAND ----------

# Writing the orders dataFrame as a parqut file in the silver layer, should use mode = 'overwrite' in this instance
orders.write.parquet("/FileStore/tables/silver/orders", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ## ORDER_ITEMS
# MAGIC Save a table called ORDER_ITEMS iin the silver layer, it should contain the following columns:
# MAGIC - ORDER_ID, type INTEGER
# MAGIC - PRODUCT_ID, type INTEGER
# MAGIC - PUNIT_PRICE, type DOUBLE
# MAGIC - QUANTITY, type INTEGER
# MAGIC
# MAGIC The file should be saved in PARQUET format
# MAGIC

# COMMAND ----------

# Reading the order items csv file
order_items_path = "/FileStore/tables/bronze/order_items.csv"

order_items_schema = StructType([
    StructField("ORDER_ID", IntegerType(), False),
    StructField("LINE_ITEM_ID", StringType(), False),
    StructField("PRODUCT_ID", IntegerType(), False),
    StructField("UNIT_PRICE", DoubleType(), False),
    StructField("QUANTITY", IntegerType(), False)
])

order_items = spark.read.csv(path=order_items_path, header=True, schema=order_items_schema)

# COMMAND ----------

order_items.display()

# COMMAND ----------

# Selecting only the required columns and assigning this back to the order_items variable
order_items = order_items.drop('LINE_ITEM_ID')

# COMMAND ----------

#Reviewing the order_items dataframe
order_items.display()

# COMMAND ----------

# Writing the order_items parquet table in silver layer
order_items.write.parquet("/FileStore/tables/silver/order_items", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ## PRODUCTS
# MAGIC Save a table called PRODUCTS in the silver layer. It should contain the following columns:
# MAGIC - PRODUCT_ID, type INTEGER
# MAGIC - PRODUCT_NAME, type STRING
# MAGIC - UNIT_PRICE, type DOUBLE
# MAGIC
# MAGIC The file should be saved in PARQUET format
# MAGIC

# COMMAND ----------

# Reading the products csv file

products_path = "/FileStore/tables/bronze/products.csv"

products_schema = StructType([
    StructField("PRODUCT_ID", IntegerType(), False),
    StructField("PRODUCT_NAME", StringType(), False),
    StructField("UNIT_PRICE", DoubleType(), False)
])

products = spark.read.csv(path=products_path, header=True, schema=products_schema)

# COMMAND ----------

# reviewing the records
products.display()

# COMMAND ----------

# writing the parquet file
products.write.parquet("/FileStore/tables/silver/products", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ## CUSTOMERS
# MAGIC Save a table called CUSTOMERS in the silver layer, it should contain the following columns:
# MAGIC - CUSTOMER_ID, type INTEGER
# MAGIC - FULL_NAME, type STRING
# MAGIC - EMAIL_ADDRESS, type Double
# MAGIC
# MAGIC The file should be saved in PARQUET format
# MAGIC
# MAGIC

# COMMAND ----------

# Reading the customers csv file
customers_path = "/FileStore/tables/bronze/customers.csv"

customers_schema = StructType(
    [
        StructField("CUSTOMER_ID", IntegerType(), False),
        StructField("FULL_NAME", StringType(), False),
        StructField("EMAIL_ADDRESS", StringType(), False)
    ]
)

customers = spark.read.csv(path=customers_path, header=True, schema=customers_schema)

# COMMAND ----------

# reviewing the dataframe
customers.display()

# COMMAND ----------

# writing the parquet file
customers.write.parquet('/FileStore/tables/silver/customers', mode='overwrite')

# COMMAND ----------

