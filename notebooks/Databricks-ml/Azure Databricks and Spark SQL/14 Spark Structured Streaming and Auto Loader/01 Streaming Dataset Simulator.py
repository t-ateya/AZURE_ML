# Databricks notebook source
# MAGIC %md
# MAGIC Streaming Dataset Simulator
# MAGIC z
# MAGIC This notebook is mounting the new container into DBFS and then simulating a streaming dataset by incrementally loading a single at a time
# MAGIC

# COMMAND ----------

# Reaing the full orders dataset into a Dataframe called order_full
orders_full = spark.read.csv('/mnt/streaming/full_dataset/orders_full.csv', header=True)

# COMMAND ----------

# Displaying the full orders dataset
orders_full.display()

# COMMAND ----------

# You ,can filter a single order from the full dataset by using the filter method
orders_full.filter(orders_full['ORDER_ID']==1).display()

# COMMAND ----------

# Loading the first order into the streaming dataset
order_1 = orders_full.filter(orders_full['ORDER_ID']==1)
order_1.write.options(header=True).mode('append').csv("/mnt/streaming/streaming_dataset/orders_streaming.csv")

# COMMAND ----------

# Reading the orders_streaming dataset via a DataFrame
orders_streaming = spark.read.csv("/mnt/streaming/streaming_dataset/orders_streaming.csv", header=True)
orders_streaming.display()


# COMMAND ----------

# Loading the first order into the streaming dataset
order_2 = orders_full.filter(orders_full['ORDER_ID']==2)
order_2.write.options(header=True).mode('append').csv("/mnt/streaming/streaming_dataset/orders_streaming.csv")

# COMMAND ----------

orders_streaming.display()


# COMMAND ----------

# Loading the third order into the streaming dataset
order_3 = orders_full.filter(orders_full['ORDER_ID'] == 3)
order_3.write.options(header=True).mode('append').csv('/mnt/streaming/streaming_dataset/orders_streaming.csv')


# COMMAND ----------

# Reading the orders_streaming dataset via a DataFrame
orders_streaming = spark.read.csv("/mnt/streaming/streaming_dataset/orders_streaming.csv", header = True)
orders_streaming.display()


# COMMAND ----------

# Loading the 4th and 5th order into the streaming dataset 
orders_4_5 = orders_full.filter(orders_full['ORDER_ID'] == 4  | (orders_full['ORDER_ID'] == 5))
orders_4_5.write.options(headers=True).mode('append').csv("/mnt/streaming/streaming_dataset/orders_streaming.csv")

# COMMAND ----------

orders_streaming = spark.read.csv("/mnt/streaming/streaming_dataset/orders_streaming.csv", header = True)
orders_streaming.display()

# COMMAND ----------

# Loading the 6th and 7th order into the streaming dataset
orders_6_7 = orders_full.filter((orders_full['ORDER_ID'] ==6)  | (orders_full['ORDER_ID'] == 7))
orders_6_7.write.options(header=True).mode('append').csv("/mnt/streaming/streaming_dataset/orders_streaming.csv")

# COMMAND ----------

# Reading the orders_streaming dataset via a DataFrame
orders_streaming = spark.read.csv("/mnt/streaming/streaming_dataset/orders_streaming.csv", header = True)
orders_streaming.display()

# COMMAND ----------

# Deleting the streaming dataset
dbutils.fs.rm("/mnt/streaming/streaming_dataset/orders_streaming.csv", recurse = True)

# COMMAND ----------

