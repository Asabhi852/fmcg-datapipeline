# Databricks notebook source
from pyspark.sql import functions as f
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/rohanmanasa26@gmail.com/consolidated_pipeline/setup/alis

# COMMAND ----------

dbutils.widgets.text("catalog","abhi","catalog")
dbutils.widgets.text("data_source","products","data_source")
catalog=dbutils.widgets.get("catalog")
data_source=dbutils.widgets.get("data_source")

base=f"s3://fmcg-project-data-pipeline/{data_source}/*.csv"
print(base)


# COMMAND ----------

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(base)
df.printSchema()


# COMMAND ----------


df.write.format("delta").option("delta.enableChangeDataFeed","true").mode("overwrite").saveAsTable(f"{catalog}.bronze.new_{data_source}")
df.display()


# COMMAND ----------

df_bronze=spark.table(f"{catalog}.bronze.{data_source}")
df_bronze.display()

# COMMAND ----------

df_silver=df_bronze.withColumnRenamed("product_name0","product").withColumnRenamed("product_name1","product_id")


# COMMAND ----------

df_silver.limit(10).display()

# COMMAND ----------

df_silver=df_silver.withColumn("category",f.initcap(f.col("category"))).withColumn("product_id",f.when(f.col("product_id").rlike("^[0-9]+$"),f.col("product_id")).otherwise(f.lit("9999999")).cast("string"))


# COMMAND ----------

df_silver=df_silver.dropDuplicates(["product_id"])

# COMMAND ----------

df_silver.limit(10).display()

# COMMAND ----------

df_silver=df_silver.withColumn("variant",f.regexp_extract(f.col("product"),r"\((.*?)\)", 1)).withColumn("division",f.lit("Sports Bar"))

df_silver = (
    df_silver
    .withColumn(
        "product",
        f.regexp_replace(f.col("product"), "(?i)Protien", "Protein")
    )
    .withColumn(
        "category",
        f.regexp_replace(f.col("category"), "(?i)Protien", "Protein")
    )
)


df_silver.limit(10).display()


# COMMAND ----------

df_silver=df_silver.withColumn("product_code",f.sha2(f.col("product"),256).cast("string"))
df_silver.limit(10).display()

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
df_silver.write.format("delta").option("delta.enableChangeDataFeed","true").option("mergeSchema","true").mode("overwrite").saveAsTable(f"{catalog}.silver.new_{data_source}")


# COMMAND ----------

df_gold=spark.table(f"{catalog}.silver.{data_source}")
df_gold.display()


# COMMAND ----------

df_gold.write.format("delta").option("delta.enableChangeDataFeed","true").mode("overwrite").saveAsTable(f"{catalog}.gold.new_{data_source}")

# COMMAND ----------

df_gold=df_gold.select("product_code","division","category","product","variant")
df_gold.limit(10).display()



# COMMAND ----------


df_gold=df_gold.dropDuplicates(["product_code"])

# COMMAND ----------

delta_table=DeltaTable.forName(spark,f"{catalog}.gold.dim_{data_source}")
delta_table.alias("t").merge(df_gold.alias("s"),condition="t.product_code=s.product_code").whenMatchedUpdate(set={"t.division":"s.division","t.category":"s.category","t.product":"s.product","t.variant":"s.variant"}).whenNotMatchedInsert(
    values={
        "t.product_code":"s.product_code",
        "t.division":"s.division",
        "t.category":"s.category",
        "t.product":"s.product",
        "t.variant":"s.variant"
    }
).execute()


# COMMAND ----------

