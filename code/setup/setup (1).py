# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists abhi;
# MAGIC use catalog abhi;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists bronze;
# MAGIC create schema if not exists silver;
# MAGIC create schema if not exists gold;
# MAGIC

# COMMAND ----------

