# Databricks notebook source
# DBTITLE 1,File Location

/FileStore/tables/googleplaystore.csv

# COMMAND ----------

# DBTITLE 1,Import  Library
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Create dataframe
df = spark.read.load('/FileStore/tables/googleplaystore.csv',format='csv',sep=',',header='True',escape='"',inferSchema='True')

# COMMAND ----------

df.count()

# COMMAND ----------

df.show(1)

# COMMAND ----------

# DBTITLE 1,Check Schema
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Data Cleaning
df=df.drop('size','Content Rating','Last Upfrdated','Android Ver')

# COMMAND ----------

df.show(2)

# COMMAND ----------

df=df.drop('Current Ver')

# COMMAND ----------

df.show(2)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

df=df.withColumn("Reviews",col("Reviews").cast(IntegerType()))\
.withColumn("Installs",regexp_replace(col("Installs"),"[^0-9]",""))\
.withColumn("Installs",col("Installs").cast(IntegerType()))\
            .withColumn("Price",regexp_replace(col("Price"),"[$]",""))\
               .withColumn("Price",col("Price").cast(IntegerType()))

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("apps")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from apps

# COMMAND ----------

# DBTITLE 1,Top reviews given to the apps
# MAGIC %sql 
# MAGIC select App,sum(Reviews) from apps
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# DBTITLE 1,Top 10 installs app
# MAGIC %sql 
# MAGIC select App,Type, sum(Installs) from apps
# MAGIC group by 1,2
# MAGIC order by 3 desc

# COMMAND ----------

# DBTITLE 1,Category wise distribution
# MAGIC %sql 
# MAGIC select category, sum(Installs) from apps
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# DBTITLE 1,Top paid apps
# MAGIC %sql 
# MAGIC select App,sum(Price) from apps
# MAGIC where type = 'Paid'
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------


