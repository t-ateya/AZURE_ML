# Databricks notebook source
# MAGIC %pip install mlflow

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 1: Dataset Overview

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2: Load Data

# COMMAND ----------

from pyspark.sql.types import DoubleType, StringType, StructField, StructType
#NB; False implies columns cannot contain null values
schema = StructType([
  StructField("age", DoubleType(), False),
  StructField("workclass", StringType(), False),
  StructField("fnlwgt", DoubleType(), False),
  StructField("education", StringType(), False),
  StructField("education_num", DoubleType(), False),
  StructField("marital_status", StringType(), False),
  StructField("occupation", StringType(), False),
  StructField("relationship", StringType(), False),
  StructField("race", StringType(), False),
  StructField("sex", StringType(), False),
  StructField("capital_gain", DoubleType(), False),
  StructField("capital_loss", DoubleType(), False),
  StructField("hours_per_week", DoubleType(), False),
  StructField("native_country", StringType(), False),
  StructField("income", StringType(), False)
])
 
dataset = spark.read.format("csv").schema(schema).load("/FileStore/tables/adult.data")

# COMMAND ----------

display(dataset)

# COMMAND ----------

type(dataset)

# COMMAND ----------

cols = dataset.columns
cols

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 3: Data Preprocessing & Feature Engineering
# MAGIC
# MAGIC `Steps:`
# MAGIC - Encoding the categorical columns (Features & Label Column)
# MAGIC - Transform features into a vector
# MAGIC - Run stages as a Pipeline
# MAGIC - Keep relevant Columns
# MAGIC - Split data into training and test sets

# COMMAND ----------

# MAGIC %md
# MAGIC To use algorithms like Logistic Regression, you must first convert the categorical variables in the dataset into numeric variables. There are two ways to do this.
# MAGIC
# MAGIC Category Indexing
# MAGIC
# MAGIC This is basically assigning a numeric value to each category from {0, 1, 2, ...numCategories-1}. This introduces an implicit ordering among your categories, and is more suitable for ordinal variables (eg: Poor: 0, Average: 1, Good: 2)
# MAGIC
# MAGIC One-Hot Encoding
# MAGIC
# MAGIC This converts categories into binary vectors with at most one nonzero value (eg: (Blue: [1, 0]), (Green: [0, 1]), (Red: [0, 0]))
# MAGIC
# MAGIC In this notebook uses a combination of StringIndexer and, depending on your Spark version, either OneHotEncoder or OneHotEncoderEstimator to convert the categorical variables. OneHotEncoder and OneHotEncoderEstimator return a SparseVector.
# MAGIC
# MAGIC Since there is more than one stage of feature transformations, use a Pipeline to tie the stages together. This simplifies the code.
# MAGIC
# MAGIC

# COMMAND ----------

import pyspark
from pyspark.ml import Pipeline 
from pyspark.ml.feature import StringIndexer, VectorAssembler
from distutils.version import LooseVersion #Used to compare and manipulate the version numbers.

# COMMAND ----------

categorical_columns = ["workclass", "education", "marital_status", "occupation", "relationship", "race", "sex", "relative_country"]
print(categorical_columns)

# COMMAND ----------

stages = [] # stages in Pipeline

for categorical_column in categorical_columns:

    # Categorical Indexing with StringIndexer
    stringIndexer = StringIndexer(inputCol=categorical_column, outputCol=categorical_column + "Index")

    # Use OneHotEncoder to convert categorical variables into binary SpareVectors
    if LooseVersion(pyspark.__version__) < LooseVersion("3.0"):
        from pyspark.ml.feature import OneHotEncoderEstimator
        encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols = [categorical_column + "classVec"]) 
    else:
        from pyspark.ml.feature import OneHotEncoder 
        encoder = OneHotEncoder(inputCols=[stringIndexer.getOutputCol()], outputCols = [categorical_column + "classVec"])
    
    # Add stages, these are not here, we will run all at once later on
    stages += [stringIndexer, encoder]
    



# COMMAND ----------

# Convert label into label indices using the StringIndexer
label_stringIdx = StringIndexer(inputCol="income", outputCol="label")
stages += [label_stringIdx]

# COMMAND ----------

stages

# COMMAND ----------

