# Databricks notebook source


spark.conf.set(
  "fs.azure.account.key.retailstorage777.dfs.core.windows.net",
  "Access Key"
)


# COMMAND ----------
# Explore Data Lake structure (Bronze Layer)

dbutils.fs.ls("abfss://retaildata@retailstorage777.dfs.core.windows.net/")


# COMMAND ----------

dbutils.fs.ls("abfss://retaildata@retailstorage777.dfs.core.windows.net/BronzeLayer/")


# COMMAND ----------
# Read Bronze Layer datasets
# Bronze = Raw ingested data from ADF pipeline

dbutils.fs.ls("abfss://retaildata@retailstorage777.dfs.core.windows.net/BronzeLayer/Product/")


# COMMAND ----------

df = spark.read.parquet(
  "abfss://retaildata@retailstorage777.dfs.core.windows.net/BronzeLayer/Product/"
)




# COMMAND ----------

display(df)

# COMMAND ----------
# Read Bronze Layer datasets
# Bronze = Raw ingested data from ADF pipeline

# COMMAND ----------

df_transactions=spark.read.parquet(
  "abfss://retaildata@retailstorage777.dfs.core.windows.net/BronzeLayer/Transaction/"
)

df_products=spark.read.parquet(
  "abfss://retaildata@retailstorage777.dfs.core.windows.net/BronzeLayer/Product/"
)

df_stores=spark.read.parquet(
  "abfss://retaildata@retailstorage777.dfs.core.windows.net/BronzeLayer/Store/"
)


# COMMAND ----------

df_customers=spark.read.parquet(
  "abfss://retaildata@retailstorage777.dfs.core.windows.net/BronzeLayer/Customer/manish040596/azure-data-engineer---multi-source/refs/heads/main/"
)

# COMMAND ----------

display(df_customers)

# COMMAND ----------
# Data Cleaning & Type Standardization (Silver Layer Prep)
# Silver = Cleaned & transformed data

from pyspark.sql.functions import col

# Convert types and clean data
df_transactions = df_transactions.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date")
)

df_products = df_products.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("category"),
    col("price").cast("double")
)

df_stores = df_stores.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)

df_customers = df_customers.select(
    "customer_id", "first_name", "last_name", "email", "city", "registration_date"
).dropDuplicates(["customer_id"])


# COMMAND ----------


# Build Silver Layer (Unified Analytical Dataset)
# Join transactional & master datasets
df_silver = df_transactions \
    .join(df_customers, "customer_id") \
    .join(df_products, "product_id") \
    .join(df_stores, "store_id") \
    .withColumn("total_amount", col("quantity") * col("price"))


# COMMAND ----------

display(df_silver)

# COMMAND ----------

# Persist Silver Layer
# Delta format = Optimized for analytics & ACID compliance

silver_path = "abfss://retaildata@retailstorage777.dfs.core.windows.net/SilverLayer/"



# COMMAND ----------

df_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .save(silver_path)

# COMMAND ----------

df_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("retail_silver_cleaned")


# COMMAND ----------

silver_df = spark.table("retail_silver_cleaned")


# COMMAND ----------

# Build Gold Layer (Business-Level Aggregations)
# Gold = Reporting / BI optimized dataset

from pyspark.sql.functions import sum, countDistinct, avg

gold_df = silver_df.groupBy(
    "transaction_date",
    "product_id", "product_name", "category",
    "store_id", "store_name", "location"
).agg(
    sum("quantity").alias("total_quantity_sold"),
    sum("total_amount").alias("total_sales_amount"),
    countDistinct("transaction_id").alias("number_of_transactions"),
    avg("total_amount").alias("average_transaction_value")
)


# COMMAND ----------

display(gold_df)


# COMMAND ----------

gold_path = "abfss://retaildata@retailstorage777.dfs.core.windows.net/Goldlayer/"


# COMMAND ----------

gold_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save(gold_path)


# COMMAND ----------

gold_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("retail_gold_sales_summary")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_gold_sales_summary

# COMMAND ----------

