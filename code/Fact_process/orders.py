# Databricks notebook source
from pyspark.sql import functions as f
from delta.tables import DeltaTable
from pyspark.sql.window import Window




# COMMAND ----------

# MAGIC %run /Workspace/Users/rohanmanasa26@gmail.com/consolidated_pipeline/setup/alis

# COMMAND ----------

dbutils.widgets.text("catalog","abhi","catalog")
dbutils.widgets.text("data_source","orders","data_source")
catalog=dbutils.widgets.get("catalog")
data_source=dbutils.widgets.get("data_source")

base=f"s3://fmcg-project-data-pipeline/{data_source}"
landing=f"{base}/landing/"
processed=f"{base}/processed"
print(base)

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv(f"{landing}/*.csv")

df.printSchema()


# COMMAND ----------

df.write.format("delta").option("delta.enableChangeDataFeed","true").mode("append").saveAsTable(f"abhi.bronze.new_{data_source}")

# COMMAND ----------

files=dbutils.fs.ls(f"{base}/landing")
for files_info in files:
    dbutils.fs.mv(files_info.path,f"{processed}/{files_info.name}",True)

# COMMAND ----------

df_bronze=spark.table(f"abhi.bronze.new_{data_source}")
df_bronze.limit(10).show()

# COMMAND ----------

df_orders=df_bronze.withColumn(
    "order_placement_date",
    f.regexp_replace(f.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)
df_orders.limit(10).display()

# COMMAND ----------

# 1. Keep only rows where order_qty is present
df_orders = df_orders.filter(f.col("order_qty").isNotNull())


# 2. Clean customer_id → keep numeric, else set to 999999
df_orders = df_orders.withColumn(
    "customer_id",
    f.when(f.col("customer_id").rlike("^[0-9]+$"), f.col("customer_id"))
     .otherwise("999999")
     .cast("string")
)

# 3. Remove weekday name from the date text
#    "Tuesday, July 01, 2025" → "July 01, 2025"
df_orders = df_orders.withColumn(
    "order_placement_date",
    f.regexp_replace(f.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

# 4. Parse order_placement_date using multiple possible formats
df_orders = df_orders.withColumn(
    "order_placement_date",
    f.coalesce(
        f.try_to_date("order_placement_date", "yyyy/MM/dd"),
        f.try_to_date("order_placement_date", "dd-MM-yyyy"),
        f.try_to_date("order_placement_date", "dd/MM/yyyy"),
        f.try_to_date("order_placement_date", "MMMM dd, yyyy"),
    )
)

# 5. Drop duplicates
df_orders = df_orders.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])

# 5. convert product id to string
df_orders = df_orders.withColumn('product_id', f.col('product_id').cast('string'))

# COMMAND ----------

df_orders.show(10)

# COMMAND ----------

df_products=spark.table(f"abhi.silver.new_products").select("product_id","product_code")
df_silver=df_orders.join(df_products,on="product_id",how="inner")
df_silver.limit(10).display()

# COMMAND ----------

df_silver.write.format("delta").option("delta.enableChangeDataFeed","true").mode("overwrite").saveAsTable(f"abhi.silver.new_{data_source}")


# COMMAND ----------

df_gold=spark.table(f"abhi.silver.new_orders").select("order_placement_date","product_code","customer_id","order_qty")
df_gold.show(5)


# COMMAND ----------

df_gold=df_gold.withColumnRenamed("order_placement_date","date").withColumnRenamed("customer_id","customer_code").withColumnRenamed("order_qty","sold_quantity")
df_gold.show(10)

# COMMAND ----------

df_gold.write.format("delta").option("delta.enableChangeDataFeed","true").mode("overwrite").saveAsTable(f"abhi.gold.new_{data_source}")
delta_table=DeltaTable.forName(spark,f"abhi.gold.fact_{data_source}")
delta_table.alias("t").merge(df_gold.alias("s"),f"t.date=s.date and t.product_code=s.product_code and t.customer_code=s.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

