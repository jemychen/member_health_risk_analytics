# Databricks notebook source
# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# Load pipe-delimited CSV from volume
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", "|") \
    .load("/Volumes/Workspace/default/raw_data/beneficiary_2025.csv")

# Preview
df_raw.printSchema()
df_raw.show(5)

# Save as Delta table
df_raw.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.cms_beneficiary_raw")

print("Bronze layer loaded successfully.")